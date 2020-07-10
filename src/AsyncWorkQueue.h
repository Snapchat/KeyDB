#pragma once
#include "fastlock.h"
#include <vector>
#include <deque>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <atomic>
#include <functional>

class AsyncWorkQueue
{
    struct WorkItem
    {
        WorkItem(std::function<void()> &&fnAsync)
            : fnAsync(std::move(fnAsync))
            {}

        WorkItem(WorkItem&&) = default;
        std::function<void()> fnAsync;
    };
    std::vector<std::thread> m_vecthreads;
    std::vector<struct redisServerThreadVars*> m_vecpthreadVars;
    std::deque<WorkItem> m_workqueue;
    std::mutex m_mutex;
    std::condition_variable m_cvWakeup;
    std::atomic<bool> m_fQuitting { false };

    void WorkerThreadMain();
public:
    AsyncWorkQueue(int nthreads);
    ~AsyncWorkQueue();

    void AddWorkFunction(std::function<void()> &&fnAsync, bool fHiPri = false);
    bool removeClientAsyncWrites(struct client *c);

    void shutdown();

    void abandonThreads();
};