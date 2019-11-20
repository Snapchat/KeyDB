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
    static redisServerThreadVars vars;
    memset(&vars, 0, sizeof(redisServerThreadVars));
    serverTL = &vars;

    vars.clients_pending_asyncwrite = listCreate();

    while (!m_fQuitting)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cvWakeup.wait(lock);
        while (!m_workqueue.empty())
        {
            WorkItem task = std::move(m_workqueue.front());
            m_workqueue.pop();

            lock.unlock();
            task.fnAsync();
            lock.lock();
        }

        lock.unlock();
        aeAcquireLock();
        ProcessPendingAsyncWrites();
        aeReleaseLock();
    }

    listRelease(vars.clients_pending_asyncwrite);
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

void AsyncWorkQueue::AddWorkFunction(std::function<void()> &&fnAsync)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_workqueue.emplace(std::move(fnAsync));
    m_cvWakeup.notify_one();
}