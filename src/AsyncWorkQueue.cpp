#include "AsyncWorkQueue.h"
#include "server.h"

AsyncWorkQueue::AsyncWorkQueue(int nthreads)
{
    for (int i = 0; i < nthreads; ++i)
    {
        m_vecthreads.emplace_back([&]{
            WorkerThreadMain();
        });
    }
}

void AsyncWorkQueue::WorkerThreadMain()
{
    redisServerThreadVars vars;
    serverTL = &vars;

    vars.clients_pending_asyncwrite = listCreate();

    m_mutex.lock();
    m_vecpthreadVars.push_back(&vars);
    m_mutex.unlock();

    while (!m_fQuitting)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_workqueue.empty())
            m_cvWakeup.wait(lock);

        aeThreadOnline();
        while (!m_workqueue.empty())
        {
            WorkItem task = std::move(m_workqueue.front());
            m_workqueue.pop_front();

            lock.unlock();
            serverTL->gcEpoch = g_pserver->garbageCollector.startEpoch();
            task.fnAsync();
            g_pserver->garbageCollector.endEpoch(serverTL->gcEpoch);
            lock.lock();
        }

        lock.unlock();
        serverTL->gcEpoch = g_pserver->garbageCollector.startEpoch();
        if (listLength(serverTL->clients_pending_asyncwrite)) {
            aeAcquireLock();
            ProcessPendingAsyncWrites();
            aeReleaseLock();
        }
        g_pserver->garbageCollector.endEpoch(serverTL->gcEpoch);
        serverTL->gcEpoch.reset();
        aeThreadOffline();
    }

    listRelease(vars.clients_pending_asyncwrite);

    std::unique_lock<fastlock> lockf(serverTL->lockPendingWrite);
    serverTL->vecclientsProcess.clear();
    serverTL->clients_pending_write.clear();
    std::atomic_thread_fence(std::memory_order_seq_cst);
}

bool AsyncWorkQueue::removeClientAsyncWrites(client *c)
{
    bool fFound = false;
    aeAcquireLock();
    m_mutex.lock();
    for (auto pvars : m_vecpthreadVars)
    {
        listIter li;
        listNode *ln;
        listRewind(pvars->clients_pending_asyncwrite, &li);
        while ((ln = listNext(&li)) != nullptr)
        {
            if (c == listNodeValue(ln))
            {
                listDelNode(pvars->clients_pending_asyncwrite, ln);
                fFound = true;
            }
        }
    }
    m_mutex.unlock();
    aeReleaseLock();
    return fFound;
}

void AsyncWorkQueue::shutdown()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    serverAssert(!GlobalLocksAcquired());
    m_fQuitting = true;
    m_cvWakeup.notify_all();
    lock.unlock();

    for (auto &thread : m_vecthreads)
        thread.join();
}

void AsyncWorkQueue::abandonThreads()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_fQuitting = true;
    m_cvWakeup.notify_all();
    for (auto &thread : m_vecthreads)
    {
        thread.detach();
    }
    m_vecthreads.clear();
}

AsyncWorkQueue::~AsyncWorkQueue()
{
    serverAssert(!GlobalLocksAcquired() || m_vecthreads.empty());
    std::unique_lock<std::mutex> lock(m_mutex);
    m_fQuitting = true;
    m_cvWakeup.notify_all();
    lock.unlock();
    
    abandonThreads();
}

void AsyncWorkQueue::AddWorkFunction(std::function<void()> &&fnAsync, bool fHiPri)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    if (fHiPri)
        m_workqueue.emplace_front(std::move(fnAsync));
    else
        m_workqueue.emplace_back(std::move(fnAsync));
    m_cvWakeup.notify_one();
}