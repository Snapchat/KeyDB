#pragma once
#include <condition_variable>

class readWriteLock {
    fastlock m_readLock;
    fastlock m_writeLock;
    std::condition_variable_any m_cv;
    int m_readCount = 0;
    int m_writeCount = 0;
    bool m_writeWaiting = false;
public:
    readWriteLock(const char *name) : m_readLock(name), m_writeLock(name) {}

    void acquireRead() {
        std::unique_lock<fastlock> rm(m_readLock);
        while (m_writeCount > 0 || m_writeWaiting)
            m_cv.wait(rm);
        m_readCount++;
    }
    
    bool tryAcquireRead() {
        std::unique_lock<fastlock> rm(m_readLock, std::defer_lock);
        if (!rm.try_lock())
            return false;
        if (m_writeCount > 0 || m_writeWaiting)
            return false;
        m_readCount++;
        return true;
    }

    void acquireWrite(bool exclusive = true) {
        std::unique_lock<fastlock> rm(m_readLock);
        m_writeWaiting = true;
        while (m_readCount > 0)
            m_cv.wait(rm);
        if (exclusive) {
            /* Another thread might have the write lock while we have the read lock
               but won't be able to release it until they can acquire the read lock
               so release the read lock and try again instead of waiting to avoid deadlock */
            while(!m_writeLock.try_lock())
                m_cv.wait(rm);
        }
        m_writeCount++;
        m_writeWaiting = false;
    }

    void upgradeWrite(bool exclusive = true) {
        releaseRead();
        acquireWrite(exclusive);
    }

    bool tryAcquireWrite(bool exclusive = true) {
        std::unique_lock<fastlock> rm(m_readLock, std::defer_lock);
        if (!rm.try_lock())
            return false;
        if (m_readCount > 0)
            return false;
        if (exclusive)
            if (!m_writeLock.try_lock())
                return false;
        m_writeCount++;
        return true;
    }

    void releaseRead() {
        std::unique_lock<fastlock> rm(m_readLock);
        m_readCount--;
        m_cv.notify_all();
    }

    void releaseWrite(bool exclusive = true) {
        std::unique_lock<fastlock> rm(m_readLock);
        serverAssert(m_writeCount > 0);
        if (exclusive)
            m_writeLock.unlock();
        m_writeCount--;
        m_cv.notify_all();
    }

    void downgradeWrite(bool exclusive = true) {
        releaseWrite(exclusive);
        acquireRead();
    }

    bool hasReader() {
        return m_readCount > 0;
    }

    bool hasWriter() {
        return m_writeCount > 0;
    }

    bool writeWaiting() {
        return m_writeWaiting;
    }
};