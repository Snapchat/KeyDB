#pragma once

class AeLocker
{
    bool m_fArmed = false;

public:
    AeLocker()
    {
    }

    void arm(client *c) // if a client is passed, then the client is already locked
    {
        if (c != nullptr)
        {
            serverAssert(!m_fArmed);
            serverAssert(c->lock.fOwnLock());

            if (!aeTryAcquireLock(true /*fWeak*/))    // avoid locking the client if we can
            {
                bool fOwnClientLock = true;
                for (;;)
                {
                    if (fOwnClientLock)
                    {
                        c->lock.unlock();
                        fOwnClientLock = false;
                    }
                    aeAcquireLock();
                    if (!c->lock.try_lock(false))   // ensure a strong try because aeAcquireLock is expensive
                    {
                        aeReleaseLock();
                    }
                    else
                    {
                        break;
                    }
                }
            }
            
            m_fArmed = true;
        }
        else if (!m_fArmed)
        {
            m_fArmed = true;
            aeAcquireLock();
        }
    }

    void disarm()
    {
        serverAssert(m_fArmed);
        m_fArmed = false;
        aeReleaseLock();
    }

    bool isArmed() const
    {
        return m_fArmed;
    }

    ~AeLocker()
    {
        if (m_fArmed)
            aeReleaseLock();
    }
};