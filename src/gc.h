#pragma once
#include <vector>
#include <assert.h>

template<typename T>
class GarbageCollector
{
    struct EpochHolder
    {
        uint64_t tstamp;
        std::vector<std::unique_ptr<T>> m_vecObjs;

        bool operator<(uint64_t tstamp) const
        {
            return this->tstamp < tstamp;
        }

        bool operator==(uint64_t tstamp) const
        {
            return this->tstamp == tstamp;
        }
    };

public:
    uint64_t startEpoch()
    {
        std::unique_lock<fastlock> lock(m_lock);
        ++m_epochNext;
        m_setepochOutstanding.insert(m_epochNext);
        return m_epochNext;
    }

    void shutdown()
    {
        std::unique_lock<fastlock> lock(m_lock);
        m_vecepochs.clear();
    }

    void endEpoch(uint64_t epoch, bool fNoFree = false)
    {
        std::unique_lock<fastlock> lock(m_lock);
        assert(m_setepochOutstanding.find(epoch) != m_setepochOutstanding.end());
        bool fMinElement = *std::min_element(m_setepochOutstanding.begin(), m_setepochOutstanding.end());
        m_setepochOutstanding.erase(epoch);
        if (fNoFree)
            return;
        std::vector<EpochHolder> vecclean;
        
        // No outstanding epochs?
        if (m_setepochOutstanding.empty())
        {
            vecclean = std::move(m_vecepochs);  // Everything goes!
        }
        else
        {
            uint64_t minepoch = *std::min_element(m_setepochOutstanding.begin(), m_setepochOutstanding.end());
            if (minepoch == 0)
                return; // No available epochs to free

            // Clean any epochs available (after the lock)
            for (size_t iepoch = 0; iepoch < m_vecepochs.size(); ++iepoch)
            {
                auto &e = m_vecepochs[iepoch];
                if (e < minepoch)
                {
                    vecclean.emplace_back(std::move(e));
                    m_vecepochs.erase(m_vecepochs.begin() + iepoch);
                    --iepoch;
                }
            }

            assert(vecclean.empty() || fMinElement);
        }

        lock.unlock();  // don't hold it for the potentially long delete of vecclean
    }

    void enqueue(uint64_t epoch, std::unique_ptr<T> &&sp)
    {
        std::unique_lock<fastlock> lock(m_lock);
        assert(m_setepochOutstanding.find(epoch) != m_setepochOutstanding.end());
        assert(sp->FWillFreeChildDebug() == false);

        auto itr = std::find(m_vecepochs.begin(), m_vecepochs.end(), m_epochNext+1);
        if (itr == m_vecepochs.end())
        {
            EpochHolder e;
            e.tstamp = m_epochNext+1;
            e.m_vecObjs.push_back(std::move(sp));
            m_vecepochs.emplace_back(std::move(e));
        }
        else
        {
            itr->m_vecObjs.push_back(std::move(sp));
        }
    }

private:
    fastlock m_lock { "Garbage Collector"};

    std::vector<EpochHolder> m_vecepochs;
    std::set<uint64_t> m_setepochOutstanding;
    uint64_t m_epochNext = 0;
};