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

            bool fClientLocked = true;
            while (!aeTryAcquireLock())
            {
                if (fClientLocked) c->lock.unlock();
                fClientLocked = false;
                aeAcquireLock();
                if (!c->lock.try_lock())
                {
                    aeReleaseLock();
                }
                else
                {
                    break;
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