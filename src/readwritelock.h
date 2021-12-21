#pragma once
#include <condition_variable>

class readWriteLock {
    std::condition_variable m_cv;
    std::mutex m_readLock;
    std::mutex m_writeLock;
    std::unique_lock<std::mutex> m_ul;
    int m_readCount = 0;
    bool m_wlocked = false;
public:
    void acquireRead() {
        std::unique_lock<std::mutex> rm(m_readLock);
        m_readCount++;
    }
    void acquireWrite() {
        m_ul = std::unique_lock<std::mutex>(m_readLock);
        while (m_readCount > 0) {
            m_cv.wait(m_ul);
        }
        m_writeLock.lock();
        m_wlocked = true;
    }
    void releaseRead() {
        std::unique_lock<std::mutex> rm(m_readLock);
        m_readCount--;
        if (m_readCount == 0)
            m_cv.notify_all();
    }
    void releaseWrite() {
        serverAssert(m_wlocked);
        m_wlocked = false;
        m_writeLock.unlock();
        m_ul.unlock();
        m_cv.notify_all();
    }
};