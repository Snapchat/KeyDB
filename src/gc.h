#pragma once
#include <vector>
#include <assert.h>
#include <unordered_set>
#include <list>

struct ICollectable
{
    virtual ~ICollectable() {}
    bool FWillFreeChildDebug() { return false; }
};

template<typename T>
class GarbageCollector
{
    struct EpochHolder
    {
        uint64_t tstamp;
        std::unique_ptr<std::vector<std::unique_ptr<T>>> m_spvecObjs;

        EpochHolder() {
            m_spvecObjs = std::make_unique<std::vector<std::unique_ptr<T>>>();
        }

        // Support move operators
        EpochHolder(EpochHolder &&other) = default;
        EpochHolder &operator=(EpochHolder &&) = default;



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
    ~GarbageCollector() {
        // Silence TSAN errors
        m_lock.lock();
    }

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
        m_listepochs.clear();
        m_setepochOutstanding.clear();
    }

    bool empty() const
    {
        std::unique_lock<fastlock> lock(m_lock);
        return m_listepochs.empty();
    }

    void endEpoch(uint64_t epoch, bool fNoFree = false)
    {
        std::unique_lock<fastlock> lock(m_lock);
        serverAssert(m_setepochOutstanding.find(epoch) != m_setepochOutstanding.end());
        bool fMinElement = *std::min_element(m_setepochOutstanding.begin(), m_setepochOutstanding.end());
        m_setepochOutstanding.erase(epoch);
        if (fNoFree)
            return;
        std::list<EpochHolder> listclean;
        
        // No outstanding epochs?
        if (m_setepochOutstanding.empty())
        {
            listclean = std::move(m_listepochs);  // Everything goes!
        }
        else
        {
            uint64_t minepoch = *std::min_element(m_setepochOutstanding.begin(), m_setepochOutstanding.end());
            if (minepoch == 0)
                return; // No available epochs to free

            // Clean any epochs available (after the lock)
            for (auto itr = m_listepochs.begin(); itr != m_listepochs.end(); /* itr incremented in loop*/)
            {
                auto &e = *itr;
                auto itrNext = itr;
                ++itrNext;
                if (e < minepoch)
                {
                    listclean.emplace_back(std::move(e));
                    m_listepochs.erase(itr);
                }
                itr = itrNext;
            }

            assert(listclean.empty() || fMinElement);
        }

        lock.unlock();  // don't hold it for the potentially long delete of vecclean
    }

    void enqueue(uint64_t epoch, std::unique_ptr<T> &&sp)
    {
        std::unique_lock<fastlock> lock(m_lock);
        serverAssert(m_setepochOutstanding.find(epoch) != m_setepochOutstanding.end());
        serverAssert(sp->FWillFreeChildDebug() == false);

        auto itr = std::find(m_listepochs.begin(), m_listepochs.end(), m_epochNext+1);
        if (itr == m_listepochs.end())
        {
            EpochHolder e;
            e.tstamp = m_epochNext+1;
            e.m_spvecObjs->push_back(std::move(sp));
            m_listepochs.emplace_back(std::move(e));
        }
        else
        {
            itr->m_spvecObjs->push_back(std::move(sp));
        }
    }

private:
    mutable fastlock m_lock { "Garbage Collector"};

    std::list<EpochHolder> m_listepochs;
    std::unordered_set<uint64_t> m_setepochOutstanding;
    uint64_t m_epochNext = 0;
};
