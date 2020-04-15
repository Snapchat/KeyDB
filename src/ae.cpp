/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <condition_variable>
#include <atomic>
#include <mutex>
#include <stdio.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "ae.h"
#include "fastlock.h"
#include "zmalloc.h"
#include "config.h"

#ifdef USE_MUTEX
thread_local int cOwnLock = 0;
class mutex_wrapper
{
    std::recursive_mutex m_mutex;
public:
    void lock() {
        m_mutex.lock();
        cOwnLock++;
    }

    void unlock() {
        cOwnLock--;
        m_mutex.unlock();
    }

    bool try_lock() {
        if (m_mutex.try_lock()) {
            cOwnLock++;
            return true;
        }
        return false;
    }

    bool fOwnLock() {
        return cOwnLock > 0;
    }
};
mutex_wrapper g_lock;

#else
fastlock g_lock("AE (global)");
#endif
thread_local aeEventLoop *g_eventLoopThisThread = NULL;

#define AE_ASSERT(x) if (!(x)) do { fprintf(stderr, "AE_ASSERT FAILURE %s: %d\n", __FILE__, __LINE__); *((volatile int*)1) = 1; } while(0)

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
    #ifdef HAVE_EPOLL
    #include "ae_epoll.cpp"
    #else
        #ifdef HAVE_KQUEUE
        #include "ae_kqueue.c"
        #else
        #include "ae_select.c"
        #endif
    #endif
#endif

enum class AE_ASYNC_OP
{
    PostFunction,
    PostCppFunction,
    DeleteFileEvent,
    CreateFileEvent,
};

struct aeCommandControl
{
    std::condition_variable cv;
    std::atomic<int> rval;
    std::mutex mutexcv;
};

struct aeCommand
{
    AE_ASYNC_OP op;
    int fd; 
    int mask;
    union {
        aePostFunctionProc *proc;
        aeFileProc *fproc;
        std::function<void()> *pfn;
    };
    void *clientData;
    aeCommandControl *pctl;
};

void aeProcessCmd(aeEventLoop *eventLoop, int fd, void *, int )
{
    aeCommand cmd;
    for (;;)
    {
        auto cb = read(fd, &cmd, sizeof(aeCommand));
        if (cb != sizeof(cmd))
        {
            AE_ASSERT(errno == EAGAIN);
            break;
        }
        switch (cmd.op)
        {
        case AE_ASYNC_OP::DeleteFileEvent:
            aeDeleteFileEvent(eventLoop, cmd.fd, cmd.mask);
            break;

        case AE_ASYNC_OP::CreateFileEvent:
        {
            if (cmd.pctl != nullptr)
            {
                cmd.pctl->mutexcv.lock();
                std::atomic_store(&cmd.pctl->rval, aeCreateFileEvent(eventLoop, cmd.fd, cmd.mask, cmd.fproc, cmd.clientData));
                cmd.pctl->cv.notify_all();
                cmd.pctl->mutexcv.unlock();
            }
            else
            {
                aeCreateFileEvent(eventLoop, cmd.fd, cmd.mask, cmd.fproc, cmd.clientData);
            }
        }
            break;

        case AE_ASYNC_OP::PostFunction:
            {
            std::unique_lock<decltype(g_lock)> ulock(g_lock);
            ((aePostFunctionProc*)cmd.proc)(cmd.clientData);
            break;
            }

        case AE_ASYNC_OP::PostCppFunction:
        {
            if (cmd.pctl != nullptr)
                cmd.pctl->mutexcv.lock();
            
            std::unique_lock<decltype(g_lock)> ulock(g_lock);
            (*cmd.pfn)();
            
            if (cmd.pctl != nullptr)
            {
                cmd.pctl->cv.notify_all();
                cmd.pctl->mutexcv.unlock();
            }
            delete cmd.pfn;
        }
            break;
        }
    }
}

// Unlike write() this is an all or nothing thing.  We will block if a partial write is hit
ssize_t safe_write(int fd, const void *pv, size_t cb)
{
    const char *pcb = (const char*)pv;
    ssize_t written = 0;
    do
    {
        ssize_t rval = write(fd, pcb, cb);
        if (rval > 0)
        {
            pcb += rval;
            cb -= rval;
            written += rval;
        }
        else if (errno == EAGAIN)
        {
            if (written == 0)
                break;
            // if we've already written something then we're committed so keep trying
        }
        else
        {
            if (rval == 0)
                return written;
            return rval;
        }
    } while (cb);
    return written;
}

int aeCreateRemoteFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData, int fSynchronous)
{
    if (eventLoop == g_eventLoopThisThread)
        return aeCreateFileEvent(eventLoop, fd, mask, proc, clientData);

    int ret = AE_OK;
    
    aeCommand cmd = {};
    cmd.op = AE_ASYNC_OP::CreateFileEvent;
    cmd.fd = fd;
    cmd.mask = mask;
    cmd.fproc = proc;
    cmd.clientData = clientData;
    cmd.pctl = nullptr;
    if (fSynchronous)
    {
        cmd.pctl = new (MALLOC_LOCAL) aeCommandControl();
        cmd.pctl->mutexcv.lock();
    }

    auto size = safe_write(eventLoop->fdCmdWrite, &cmd, sizeof(cmd));
    if (size != sizeof(cmd))
    {
        AE_ASSERT(size == sizeof(cmd) || size <= 0);
        AE_ASSERT(errno == EAGAIN);
        ret = AE_ERR;
    }
    
    if (fSynchronous)
    {
        std::unique_lock<std::mutex> ulock(cmd.pctl->mutexcv, std::defer_lock);
        cmd.pctl->cv.wait(ulock);
        ret = cmd.pctl->rval;
        delete cmd.pctl;
    }

    return ret;
}

int aePostFunction(aeEventLoop *eventLoop, aePostFunctionProc *proc, void *arg)
{
    if (eventLoop == g_eventLoopThisThread)
    {
        proc(arg);
        return AE_OK;
    }
    aeCommand cmd = {};
    cmd.op = AE_ASYNC_OP::PostFunction;
    cmd.proc = proc;
    cmd.clientData = arg;
    auto size = write(eventLoop->fdCmdWrite, &cmd, sizeof(cmd));
    if (size != sizeof(cmd))
        return AE_ERR;
    return AE_OK;
}

int aePostFunction(aeEventLoop *eventLoop, std::function<void()> fn, bool fSynchronous)
{
    if (eventLoop == g_eventLoopThisThread)
    {
        fn();
        return AE_OK;
    }

    aeCommand cmd = {};
    cmd.op = AE_ASYNC_OP::PostCppFunction;
    cmd.pfn = new (MALLOC_LOCAL) std::function<void()>(fn);
    cmd.pctl = nullptr;
    if (fSynchronous)
    {
        cmd.pctl = new (MALLOC_LOCAL) aeCommandControl();
        cmd.pctl->mutexcv.lock();
    }

    auto size = write(eventLoop->fdCmdWrite, &cmd, sizeof(cmd));
    if (size != sizeof(cmd))
        return AE_ERR;
    AE_ASSERT(size == sizeof(cmd));
    int ret = AE_OK;
    if (fSynchronous)
    {
        std::unique_lock<std::mutex> ulock(cmd.pctl->mutexcv, std::defer_lock);
        cmd.pctl->cv.wait(ulock);
        ret = cmd.pctl->rval;
        delete cmd.pctl;
    }
    return ret;
}

aeEventLoop *aeCreateEventLoop(int setsize) {
    aeEventLoop *eventLoop;
    int i;

    if ((eventLoop = (aeEventLoop*)zmalloc(sizeof(*eventLoop), MALLOC_LOCAL)) == NULL) goto err;
    eventLoop->events = (aeFileEvent*)zmalloc(sizeof(aeFileEvent)*setsize, MALLOC_LOCAL);
    eventLoop->fired = (aeFiredEvent*)zmalloc(sizeof(aeFiredEvent)*setsize, MALLOC_LOCAL);
    if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;
    eventLoop->setsize = setsize;
    eventLoop->lastTime = time(NULL);
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0;
    eventLoop->maxfd = -1;
    eventLoop->beforesleep = NULL;
    eventLoop->aftersleep = NULL;
    eventLoop->flags = 0;
    if (aeApiCreate(eventLoop) == -1) goto err;
    /* Events with mask == AE_NONE are not set. So let's initialize the
     * vector with it. */
    for (i = 0; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;

    fastlock_init(&eventLoop->flock, "event loop");
    int rgfd[2];
    if (pipe(rgfd) < 0)
        goto err;
    eventLoop->fdCmdRead = rgfd[0];
    eventLoop->fdCmdWrite = rgfd[1];
    fcntl(eventLoop->fdCmdWrite, F_SETFL, O_NONBLOCK);
    fcntl(eventLoop->fdCmdRead, F_SETFL, O_NONBLOCK);
    eventLoop->cevents = 0;
    aeCreateFileEvent(eventLoop, eventLoop->fdCmdRead, AE_READABLE|AE_READ_THREADSAFE, aeProcessCmd, NULL);

    return eventLoop;

err:
    if (eventLoop) {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}

/* Return the current set size. */
int aeGetSetSize(aeEventLoop *eventLoop) {
    return eventLoop->setsize;
}

/* Tells the next iteration/s of the event processing to set timeout of 0. */
void aeSetDontWait(aeEventLoop *eventLoop, int noWait) {
    if (noWait)
        eventLoop->flags |= AE_DONT_WAIT;
    else
        eventLoop->flags &= ~AE_DONT_WAIT;
}

/* Resize the maximum set size of the event loop.
 * If the requested set size is smaller than the current set size, but
 * there is already a file descriptor in use that is >= the requested
 * set size minus one, AE_ERR is returned and the operation is not
 * performed at all.
 *
 * Otherwise AE_OK is returned and the operation is successful. */
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize) {
    AE_ASSERT(g_eventLoopThisThread == NULL || g_eventLoopThisThread == eventLoop);
    int i;

    if (setsize == eventLoop->setsize) return AE_OK;
    if (eventLoop->maxfd >= setsize) return AE_ERR;
    if (aeApiResize(eventLoop,setsize) == -1) return AE_ERR;

    eventLoop->events = (aeFileEvent*)zrealloc(eventLoop->events,sizeof(aeFileEvent)*setsize, MALLOC_LOCAL);
    eventLoop->fired = (aeFiredEvent*)zrealloc(eventLoop->fired,sizeof(aeFiredEvent)*setsize, MALLOC_LOCAL);
    eventLoop->setsize = setsize;

    /* Make sure that if we created new slots, they are initialized with
     * an AE_NONE mask. */
    for (i = eventLoop->maxfd+1; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return AE_OK;
}

extern "C" void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    aeApiFree(eventLoop);
    zfree(eventLoop->events);
    zfree(eventLoop->fired);
    fastlock_free(&eventLoop->flock);
    close(eventLoop->fdCmdRead);
    close(eventLoop->fdCmdWrite);

    /* Free the time events list. */
    auto *te = eventLoop->timeEventHead;
    while (te)
    {
        auto *teNext = te->next;
        zfree(te);
        te = teNext;
    }
    zfree(eventLoop);
}

extern "C" void aeStop(aeEventLoop *eventLoop) {
    AE_ASSERT(g_eventLoopThisThread == NULL || g_eventLoopThisThread == eventLoop);
    eventLoop->stop = 1;
}

extern "C" int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
        aeFileProc *proc, void *clientData)
{
    AE_ASSERT(g_eventLoopThisThread == NULL || g_eventLoopThisThread == eventLoop);
    if (fd >= eventLoop->setsize) {
        errno = ERANGE;
        return AE_ERR;
    }

    aeFileEvent *fe = &eventLoop->events[fd];

    if (aeApiAddEvent(eventLoop, fd, mask) == -1)
        return AE_ERR;
    fe->mask |= mask;
    if (mask & AE_READABLE) fe->rfileProc = proc;
    if (mask & AE_WRITABLE) fe->wfileProc = proc;
    fe->clientData = clientData;
    if (fd > eventLoop->maxfd)
        eventLoop->maxfd = fd;
    return AE_OK;
}

void aeDeleteFileEventAsync(aeEventLoop *eventLoop, int fd, int mask)
{
    if (eventLoop == g_eventLoopThisThread)
        return aeDeleteFileEvent(eventLoop, fd, mask);
    aeCommand cmd;
    cmd.op = AE_ASYNC_OP::DeleteFileEvent;
    cmd.fd = fd;
    cmd.mask = mask;
    auto cb = write(eventLoop->fdCmdWrite, &cmd, sizeof(cmd));
    AE_ASSERT(cb == sizeof(cmd));
}

extern "C" void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask)
{
    AE_ASSERT(g_eventLoopThisThread == NULL || g_eventLoopThisThread == eventLoop);
    if (fd >= eventLoop->setsize) return;
    aeFileEvent *fe = &eventLoop->events[fd];
    if (fe->mask == AE_NONE) return;

    /* We want to always remove AE_BARRIER if set when AE_WRITABLE
     * is removed. */
    if (mask & AE_WRITABLE) mask |= AE_BARRIER;

    if (mask & AE_WRITABLE) mask |= AE_WRITE_THREADSAFE;
    if (mask & AE_READABLE) mask |= AE_READ_THREADSAFE;

    aeApiDelEvent(eventLoop, fd, mask);
    fe->mask = fe->mask & (~mask);
    if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
        /* Update the max fd */
        int j;

        for (j = eventLoop->maxfd-1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE) break;
        eventLoop->maxfd = j;
    }
}

extern "C" int aeGetFileEvents(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return 0;
    aeFileEvent *fe = &eventLoop->events[fd];

    return fe->mask;
}

static void aeGetTime(long *seconds, long *milliseconds)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    *seconds = tv.tv_sec;
    *milliseconds = tv.tv_usec/1000;
}

static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms) {
    long cur_sec, cur_ms, when_sec, when_ms;

    aeGetTime(&cur_sec, &cur_ms);
    when_sec = cur_sec + milliseconds/1000;
    when_ms = cur_ms + milliseconds%1000;
    if (when_ms >= 1000) {
        when_sec ++;
        when_ms -= 1000;
    }
    *sec = when_sec;
    *ms = when_ms;
}

extern "C" long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
        aeTimeProc *proc, void *clientData,
        aeEventFinalizerProc *finalizerProc)
{
    AE_ASSERT(g_eventLoopThisThread == NULL || g_eventLoopThisThread == eventLoop);
    long long id = eventLoop->timeEventNextId++;
    aeTimeEvent *te;

    te = (aeTimeEvent*)zmalloc(sizeof(*te), MALLOC_LOCAL);
    if (te == NULL) return AE_ERR;
    te->id = id;
    aeAddMillisecondsToNow(milliseconds,&te->when_sec,&te->when_ms);
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    te->prev = NULL;
    te->next = eventLoop->timeEventHead;
    if (te->next)
        te->next->prev = te;
    eventLoop->timeEventHead = te;
    return id;
}

extern "C" int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
    AE_ASSERT(g_eventLoopThisThread == NULL || g_eventLoopThisThread == eventLoop);
    aeTimeEvent *te = eventLoop->timeEventHead;
    while(te) {
        if (te->id == id) {
            te->id = AE_DELETED_EVENT_ID;
            return AE_OK;
        }
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}

/* Search the first timer to fire.
 * This operation is useful to know how many time the select can be
 * put in sleep without to delay any event.
 * If there are no timers NULL is returned.
 *
 * Note that's O(N) since time events are unsorted.
 * Possible optimizations (not needed by Redis so far, but...):
 * 1) Insert the event in order, so that the nearest is just the head.
 *    Much better but still insertion or deletion of timers is O(N).
 * 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
 */
static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop)
{
    AE_ASSERT(g_eventLoopThisThread == NULL || g_eventLoopThisThread == eventLoop);
    aeTimeEvent *te = eventLoop->timeEventHead;
    aeTimeEvent *nearest = NULL;

    while(te) {
        if (!nearest || te->when_sec < nearest->when_sec ||
                (te->when_sec == nearest->when_sec &&
                 te->when_ms < nearest->when_ms))
            nearest = te;
        te = te->next;
    }
    return nearest;
}

/* Process time events */
static int processTimeEvents(aeEventLoop *eventLoop) {
    std::unique_lock<decltype(g_lock)> ulock(g_lock);
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;
    time_t now = time(NULL);

    /* If the system clock is moved to the future, and then set back to the
     * right value, time events may be delayed in a random way. Often this
     * means that scheduled operations will not be performed soon enough.
     *
     * Here we try to detect system clock skews, and force all the time
     * events to be processed ASAP when this happens: the idea is that
     * processing events earlier is less dangerous than delaying them
     * indefinitely, and practice suggests it is. */
    if (now < eventLoop->lastTime) {
        te = eventLoop->timeEventHead;
        while(te) {
            te->when_sec = 0;
            te = te->next;
        }
    }
    eventLoop->lastTime = now;

    te = eventLoop->timeEventHead;
    maxId = eventLoop->timeEventNextId-1;
    while(te) {
        long now_sec, now_ms;
        long long id;

        /* Remove events scheduled for deletion. */
        if (te->id == AE_DELETED_EVENT_ID) {
            aeTimeEvent *next = te->next;
            if (te->prev)
                te->prev->next = te->next;
            else
                eventLoop->timeEventHead = te->next;
            if (te->next)
                te->next->prev = te->prev;
            if (te->finalizerProc)
                te->finalizerProc(eventLoop, te->clientData);
            zfree(te);
            te = next;
            continue;
        }

        /* Make sure we don't process time events created by time events in
         * this iteration. Note that this check is currently useless: we always
         * add new timers on the head, however if we change the implementation
         * detail, this check may be useful again: we keep it here for future
         * defense. */
        if (te->id > maxId) {
            te = te->next;
            continue;
        }
        aeGetTime(&now_sec, &now_ms);
        if (now_sec > te->when_sec ||
            (now_sec == te->when_sec && now_ms >= te->when_ms))
        {
            int retval;

            id = te->id;
            retval = te->timeProc(eventLoop, id, te->clientData);
            processed++;
            if (retval != AE_NOMORE) {
                aeAddMillisecondsToNow(retval,&te->when_sec,&te->when_ms);
            } else {
                te->id = AE_DELETED_EVENT_ID;
            }
        }
        te = te->next;
    }
    return processed;
}

extern "C" void ProcessEventCore(aeEventLoop *eventLoop, aeFileEvent *fe, int mask, int fd)
{
#define LOCK_IF_NECESSARY(fe, tsmask) \
    std::unique_lock<decltype(g_lock)> ulock(g_lock, std::defer_lock); \
    if (!(fe->mask & tsmask)) \
        ulock.lock()

    int fired = 0; /* Number of events fired for current fd. */

    /* Normally we execute the readable event first, and the writable
    * event laster. This is useful as sometimes we may be able
    * to serve the reply of a query immediately after processing the
    * query.
    *
    * However if AE_BARRIER is set in the mask, our application is
    * asking us to do the reverse: never fire the writable event
    * after the readable. In such a case, we invert the calls.
    * This is useful when, for instance, we want to do things
    * in the beforeSleep() hook, like fsynching a file to disk,
    * before replying to a client. */
    int invert = fe->mask & AE_BARRIER;

    /* Note the "fe->mask & mask & ..." code: maybe an already
        * processed event removed an element that fired and we still
        * didn't processed, so we check if the event is still valid.
        *
        * Fire the readable event if the call sequence is not
        * inverted. */
    if (!invert && fe->mask & mask & AE_READABLE) {
        LOCK_IF_NECESSARY(fe, AE_READ_THREADSAFE);
        fe->rfileProc(eventLoop,fd,fe->clientData,mask | (fe->mask & AE_READ_THREADSAFE));
        fired++;
        fe = &eventLoop->events[fd]; /* Refresh in case of resize. */
    }

    /* Fire the writable event. */
    if (fe->mask & mask & AE_WRITABLE) {
        if (!fired || fe->wfileProc != fe->rfileProc) {
            LOCK_IF_NECESSARY(fe, AE_WRITE_THREADSAFE);
            fe->wfileProc(eventLoop,fd,fe->clientData,mask | (fe->mask & AE_WRITE_THREADSAFE));
            fired++;
        }
    }

    /* If we have to invert the call, fire the readable event now
        * after the writable one. */
    if (invert && fe->mask & mask & AE_READABLE) {
        if (!fired || fe->wfileProc != fe->rfileProc) {
            LOCK_IF_NECESSARY(fe, AE_READ_THREADSAFE);
            fe->rfileProc(eventLoop,fd,fe->clientData,mask | (fe->mask & AE_READ_THREADSAFE));
            fired++;
        }
    }

#undef LOCK_IF_NECESSARY
}

/* Process every pending time event, then every pending file event
 * (that may be registered by time event callbacks just processed).
 * Without special flags the function sleeps until some file event
 * fires, or when the next time event occurs (if any).
 *
 * If flags is 0, the function does nothing and returns.
 * if flags has AE_ALL_EVENTS set, all the kind of events are processed.
 * if flags has AE_FILE_EVENTS set, file events are processed.
 * if flags has AE_TIME_EVENTS set, time events are processed.
 * if flags has AE_DONT_WAIT set the function returns ASAP until all
 * the events that's possible to process without to wait are processed.
 * if flags has AE_CALL_AFTER_SLEEP set, the aftersleep callback is called.
 *
 * The function returns the number of events processed. */
int aeProcessEvents(aeEventLoop *eventLoop, int flags)
{
    AE_ASSERT(g_eventLoopThisThread == NULL || g_eventLoopThisThread == eventLoop);
    int processed = 0, numevents;

    /* Nothing to do? return ASAP */
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    /* Note that we want call select() even if there are no
     * file events to process as long as we want to process time
     * events, in order to sleep until the next time event is ready
     * to fire. */
    if (eventLoop->maxfd != -1 ||
        ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) {
        int j;
        aeTimeEvent *shortest = NULL;
        struct timeval tv, *tvp;

        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
            shortest = aeSearchNearestTimer(eventLoop);
        if (shortest) {
            long now_sec, now_ms;

            aeGetTime(&now_sec, &now_ms);
            tvp = &tv;

            /* How many milliseconds we need to wait for the next
             * time event to fire? */
            long long ms =
                (shortest->when_sec - now_sec)*1000 +
                shortest->when_ms - now_ms;

            if (ms > 0) {
                tvp->tv_sec = ms/1000;
                tvp->tv_usec = (ms % 1000)*1000;
            } else {
                tvp->tv_sec = 0;
                tvp->tv_usec = 0;
            }
        } else {
            /* If we have to check for events but need to return
             * ASAP because of AE_DONT_WAIT we need to set the timeout
             * to zero */
            if (flags & AE_DONT_WAIT) {
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            } else {
                /* Otherwise we can block */
                tvp = NULL; /* wait forever */
            }
        }

        if (eventLoop->flags & AE_DONT_WAIT) {
            tv.tv_sec = tv.tv_usec = 0;
            tvp = &tv;
        }

        /* Call the multiplexing API, will return only on timeout or when
         * some event fires. */
        numevents = aeApiPoll(eventLoop, tvp);

        /* After sleep callback. */
        if (eventLoop->aftersleep != NULL && flags & AE_CALL_AFTER_SLEEP) {
            std::unique_lock<decltype(g_lock)> ulock(g_lock, std::defer_lock);
            if (!(eventLoop->aftersleepFlags & AE_SLEEP_THREADSAFE))
                ulock.lock();
            eventLoop->aftersleep(eventLoop);
        }

        for (j = 0; j < numevents; j++) {
            aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
            int mask = eventLoop->fired[j].mask;
            int fd = eventLoop->fired[j].fd;

            ProcessEventCore(eventLoop, fe, mask, fd);

            processed++;
        }
    }
    /* Check time events */
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);

    eventLoop->cevents += processed;
    return processed; /* return the number of processed file/time events */
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
int aeWait(int fd, int mask, long long milliseconds) {
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

    if ((retval = poll(&pfd, 1, milliseconds))== 1) {
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        return retmask;
    } else {
        return retval;
    }
}

void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    g_eventLoopThisThread = eventLoop;
    while (!eventLoop->stop) {
        if (eventLoop->beforesleep != NULL) {
            std::unique_lock<decltype(g_lock)> ulock(g_lock, std::defer_lock);
            if (!(eventLoop->beforesleepFlags & AE_SLEEP_THREADSAFE))
                ulock.lock();
            eventLoop->beforesleep(eventLoop);
        }
        AE_ASSERT(!aeThreadOwnsLock()); // we should have relinquished it after processing
        aeProcessEvents(eventLoop, AE_ALL_EVENTS|AE_CALL_AFTER_SLEEP);
        AE_ASSERT(!aeThreadOwnsLock()); // we should have relinquished it after processing
    }
}

const char *aeGetApiName(void) {
    return aeApiName();
}

void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep, int flags) {
    eventLoop->beforesleep = beforesleep;
    eventLoop->beforesleepFlags = flags;
}

void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep, int flags) {
    eventLoop->aftersleep = aftersleep;
    eventLoop->aftersleepFlags = flags;
}

void aeAcquireLock()
{
    g_lock.lock();
}

int aeTryAcquireLock(int fWeak)
{
    return g_lock.try_lock(!!fWeak);
}

void aeReleaseLock()
{
    g_lock.unlock();
}

int aeThreadOwnsLock()
{
    return g_lock.fOwnLock();
}
