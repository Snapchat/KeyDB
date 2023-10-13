#pragma once
#include <condition_variable>

class readWriteLock {
    fastlock m_readLock;
    fastlock m_writeLock;
    std::condition_variable_any m_cv;
    int m_readCount = 0;
    int m_writeCount = 0;
    bool m_writeWaiting = false;
    bool m_multi = true;
public:
    readWriteLock(const char *name) : m_readLock(name), m_writeLock(name) {}

    void acquireRead() {
        std::unique_lock<fastlock> rm(m_readLock, std::defer_lock);
        if (m_multi) {
            rm.lock();
            while (m_writeCount > 0 || m_writeWaiting)
                m_cv.wait(rm);
        }
        m_readCount++;
    }
    
    bool tryAcquireRead() {
        std::unique_lock<fastlock> rm(m_readLock, std::defer_lock);
        if (m_multi) {
            if (!rm.try_lock())
                return false;
            if (m_writeCount > 0 || m_writeWaiting)
                return false;
        }
        m_readCount++;
        return true;
    }

    void acquireWrite(bool exclusive = true) {
        std::unique_lock<fastlock> rm(m_readLock, std::defer_lock);
        if (m_multi) {
            rm.lock();
            m_writeWaiting = true;
            while (m_readCount > 0)
                m_cv.wait(rm);
            if (exclusive) {
                /* Another thread might have the write lock while we have the internal lock
                but won't be able to release it until they can acquire the internal lock
                so release the internal lock and try again instead of waiting to avoid deadlock */
                while(!m_writeLock.try_lock())
                    m_cv.wait(rm);
            }
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
        if (m_multi) {
            if (!rm.try_lock())
                return false;
            if (m_readCount > 0)
                return false;
            if (exclusive)
                if (!m_writeLock.try_lock())
                    return false;
        }
        m_writeCount++;
        return true;
    }

    void releaseRead() {
        std::unique_lock<fastlock> rm(m_readLock, std::defer_lock);
        if (m_multi) {
            rm.lock();
            m_cv.notify_all();
        }
        m_readCount--;
    }

    void releaseWrite(bool exclusive = true) {
        std::unique_lock<fastlock> rm(m_readLock, std::defer_lock);
        serverAssert(m_writeCount > 0);
        if (m_multi) {
            rm.lock();
            if (exclusive)
                m_writeLock.unlock();
            m_cv.notify_all();
        }
        m_writeCount--;
    }

    void downgradeWrite(bool exclusive = true) {
        releaseWrite(exclusive);
        acquireRead();
    }

    void setMulti(bool multi) {
        m_multi = multi;
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