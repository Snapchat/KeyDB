#pragma once
#include <assert.h>
#include "compactvector.h"
#include "cowptr.h"

/****************************************
 * semiorderedset.h:
 * 
 * The ordered set is a hash set that maintains semi-ordering, that is you can iterate in sub-linear time over the set comparing a value.
 * It has a few other useful properties vs the traditional set:
 *      1. The key need not be the underlying type, the only requirement is the value type is castable to the key
 *      2. The key need not have total ordering.  The set will iterate until it finds an exact match with operator== on the value
 *          This provides additional flexibility on insert allowing us to optimize this case.
 * 
 */

extern uint64_t dictGenHashFunction(const void *key, int len);

namespace keydbutils
{
    template<typename T>
    size_t hash(const T& key)
    {
        return (size_t)dictGenHashFunction(&key, sizeof(key));
    }

    template<>
    size_t hash(const sdsview &);
}
extern size_t g_semiOrderedSetTargetBucketSize;

template<typename T, typename T_KEY = T, bool MEMMOVE_SAFE = false>
class semiorderedset
{
    typedef compactvector<T, MEMMOVE_SAFE> vector_type;
    
    friend struct setiter;
    std::vector<CowPtr<vector_type>> m_data;
    size_t celem = 0;
    static const size_t bits_min = 8;
    size_t bits = bits_min;
    size_t idxRehash = (1ULL << bits_min); 
    int cfPauseRehash = 0;

    inline size_t targetElementsPerBucket()
    {
        // Aim for roughly 4 cache lines per bucket (determined by imperical testing)
        //  lower values are faster but use more memory
        if (g_semiOrderedSetTargetBucketSize == 0)
            return std::max((64/sizeof(T))*8, (size_t)2);
        else
            return g_semiOrderedSetTargetBucketSize;
    }

public:
    semiorderedset(size_t bitsStart = 0)
    {
        if (bitsStart < bits_min)
            bitsStart = bits_min;
        bits = bitsStart;
        m_data.resize((1ULL << bits));
    }

    struct setiter
    {
        semiorderedset *set;
        size_t idxPrimary = 0;
        size_t idxSecondary = 0;

        setiter(const semiorderedset *set)
        {
            this->set = (semiorderedset*)set;
        }

        bool operator==(const setiter &other) const
        {
            return (idxPrimary == other.idxPrimary) && (idxSecondary == other.idxSecondary);
        }

        bool operator!=(const setiter &other) const { return !operator==(other); }

        inline T &operator*() { return set->m_data[idxPrimary]->operator[](idxSecondary); }
        inline const T &operator*() const { return set->m_data[idxPrimary]->operator[](idxSecondary); }

        inline T *operator->() { return &set->m_data[idxPrimary]->operator[](idxSecondary); }
        inline const T *operator->() const { return &set->m_data[idxPrimary]->operator[](idxSecondary); }
    };

    setiter find(const T_KEY &key)
    {
        RehashStep();
        return const_cast<const semiorderedset*>(this)->find(key);
    }

    setiter find(const T_KEY &key) const
    {
        setiter itr(this);
        itr.idxPrimary = idxFromObj(key);
 
        for (int hashset = 0; hashset < 2; ++hashset)   // rehashing may only be 1 resize behind, so we check up to two slots
        {
            if (m_data[itr.idxPrimary] != nullptr)
            {
                const auto &vecBucket = *m_data[itr.idxPrimary];

                auto itrFind = std::find(vecBucket.begin(), vecBucket.end(), key);
                if (itrFind != vecBucket.end())
                {
                    itr.idxSecondary =  itrFind - vecBucket.begin();
                    return itr;
                }
            }

            // See if we have to check the older slot
            size_t mask = (hashmask() >> 1);
            if (itr.idxPrimary == (itr.idxPrimary & mask))
                break;  // same bucket we just checked
            itr.idxPrimary &= mask;
            if (FRehashedRow(itr.idxPrimary))
                break;
        }
        
        return end();
    }

    bool exists(const T_KEY &key) const
    {
        auto itr = const_cast<semiorderedset<T,T_KEY,MEMMOVE_SAFE>*>(this)->find(key);
        return itr != this->end();
    }

    setiter end() const
    {
        setiter itr(const_cast<semiorderedset<T,T_KEY,MEMMOVE_SAFE>*>(this));
        itr.idxPrimary = m_data.size();
        return itr;
    }

    void insert(const T &e, bool fRehash = false)
    {
        if (!fRehash)
            RehashStep();

        auto idx = idxFromObj(static_cast<T_KEY>(e));
        if (!fRehash)
            ++celem;
        
        if (m_data[idx] == nullptr)
            m_data[idx] = std::make_shared<vector_type>();
        
        typename vector_type::iterator itrInsert;        
        if (!m_data[idx]->empty() && !(e < m_data[idx]->back()))
            itrInsert = m_data[idx]->end();
        else
            itrInsert = std::upper_bound(m_data[idx]->begin(), m_data[idx]->end(), e);
        itrInsert = m_data[idx]->insert(itrInsert, e);

        if (celem > ((1ULL << bits)*targetElementsPerBucket()))
            grow();
    }

    // enumeration starting from the 'itrStart'th key.  Note that the iter is a hint, and need no be valid anymore
    template<typename T_VISITOR, typename T_MAX>
    setiter enumerate(const setiter &itrStart, const T_MAX &max, T_VISITOR fn, long long *pccheck)
    {
        setiter itr(itrStart);

        if (itrStart.set != this)   // really if this case isn't true its probably a bug
            itr.set = this;         // but why crash the program when we can easily fix this?
        
        cfPauseRehash++;
        if (itr.idxPrimary >= m_data.size())
            itr.idxPrimary = 0;
        
        for (size_t ibucket = 0; ibucket < m_data.size(); ++ibucket)
        {
            if (!enumerate_bucket(itr, max, fn, pccheck))
                break;
            itr.idxSecondary = 0;

            ++itr.idxPrimary;
            if (itr.idxPrimary >= m_data.size())
                itr.idxPrimary = 0;
        }
        cfPauseRehash--;
        return itr;
    }

    // This will "randomly" visit nodes biased towards lower values first
    template<typename T_VISITOR>
    size_t random_visit(T_VISITOR &fn)
    {
        bool fSawAny = true;
        size_t visited = 0;
        size_t basePrimary = rand() % m_data.size();
        for (size_t idxSecondary = 0; fSawAny; ++idxSecondary)
        {
            fSawAny = false;
            for (size_t idxPrimaryCount = 0; idxPrimaryCount < m_data.size(); ++idxPrimaryCount)
            {
                size_t idxPrimary = (basePrimary + idxPrimaryCount) % m_data.size();
                if (m_data[idxPrimary] != nullptr && idxSecondary < m_data[idxPrimary]->size())
                {
                    ++visited;
                    fSawAny = true;
                    if (!fn(m_data[idxPrimary]->operator[](idxSecondary)))
                        return visited;
                }
            }
        }
        return visited;
    }

    const T& random_value() const
    {
        assert(!empty());
        for (;;)
        {
            size_t idxPrimary = rand() % m_data.size();
            if (m_data[idxPrimary] == nullptr || m_data[idxPrimary]->empty())
                continue;

            return (*m_data[idxPrimary])[rand() % m_data[idxPrimary]->size()];
        }
    }

    void erase(const setiter &itr)
    {
        auto &vecRow = *m_data[itr.idxPrimary];
        vecRow.erase(vecRow.begin() + itr.idxSecondary);
        --celem;
        RehashStep();
    }

    void clear()
    {
        m_data = decltype(m_data)();
        bits = bits_min;
        m_data.resize(1ULL << bits);
        celem = 0;
        idxRehash = m_data.size();
    }
    
    bool empty() const noexcept { return celem == 0; }
    size_t size() const noexcept { return celem; }

    size_t estimated_bytes_used() const
    {
        // This estimate does't include all the overhead of the internal vectors
        size_t cb = sizeof(this)
            + (m_data.capacity() * sizeof(m_data[0]))
            + sizeof(T) * size();
        return cb;
    }

    #define DICT_STATS_VECTLEN 50
    size_t getstats(char *buf, size_t bufsize) const
    {
        unsigned long i, slots = 0, chainlen, maxchainlen = 0;
        unsigned long totchainlen = 0;
        unsigned long clvector[DICT_STATS_VECTLEN] = {0};
        size_t l = 0;

        if (empty()) {
            return snprintf(buf,bufsize,
                "No stats available for empty dictionaries\n");
        }

        /* Compute stats. */
        for (const auto &spvec : m_data) {
            if (spvec == nullptr)
                continue;
            const auto &vec = *spvec;
            if (vec.empty()) {
                clvector[0]++;
                continue;
            }
            slots++;
            /* For each hash entry on this slot... */
            chainlen = vec.size();
            
            clvector[(chainlen < DICT_STATS_VECTLEN) ? chainlen : (DICT_STATS_VECTLEN-1)]++;
            if (chainlen > maxchainlen) maxchainlen = chainlen;
            totchainlen += chainlen;
        }

        size_t used = m_data.size()-clvector[0];
        /* Generate human readable stats. */
        l += snprintf(buf+l,bufsize-l,
            "semiordered set stats:\n"
            " table size: %zu\n"
            " number of slots: %zu\n"
            " used slots: %ld\n"
            " max chain length: %ld\n"
            " avg chain length (counted): %.02f\n"
            " avg chain length (computed): %.02f\n"
            " Chain length distribution:\n",
            size(), used, slots, maxchainlen,
            (float)totchainlen/slots, (float)size()/m_data.size());

        for (i = 0; i < DICT_STATS_VECTLEN; i++) {
            if (clvector[i] == 0) continue;
            if (l >= bufsize) break;
            l += snprintf(buf+l,bufsize-l,
                "   %s%ld: %ld (%.02f%%)\n",
                (i == DICT_STATS_VECTLEN-1)?">= ":"",
                i, clvector[i], ((float)clvector[i]/m_data.size())*100);
        }

        /* Unlike snprintf(), teturn the number of characters actually written. */
        if (bufsize) buf[bufsize-1] = '\0';
        return strlen(buf);
    }

    void pause_rehash() { ++cfPauseRehash; }
    void unpause_rehash() { --cfPauseRehash; RehashStep(); }

private:
    inline size_t hashmask() const { return (1ULL << bits) - 1; }

    size_t idxFromObj(const T_KEY &key) const
    {
        size_t v = keydbutils::hash(key);
        return v & hashmask();
    }

    bool FRehashedRow(size_t idx) const
    {
        return (idx >= (m_data.size()/2)) || (idx < idxRehash);
    }

    void RehashStep()
    {
        if (cfPauseRehash)
            return;
        
        int steps = 0;
        for (; idxRehash < (m_data.size()/2); ++idxRehash)
        {
            if (m_data[idxRehash] == nullptr)
                continue;

            CowPtr<vector_type> spvecT;
            std::swap(m_data[idxRehash], spvecT); 

            for (const auto &v : *spvecT)
                insert(v, true);

            if (++steps > 1024)
                break;
        }
    }

    void grow()
    {
        assert(idxRehash >= (m_data.size()/2)); // we should have finished rehashing by the time we need to grow again
        
        ++bits;
        m_data.resize(1ULL << bits);
        idxRehash = 0;
        RehashStep();
    }

    template<typename T_VISITOR, typename T_MAX>
    inline bool enumerate_bucket(setiter &itr, const T_MAX &max, T_VISITOR &fn, long long *pcheckLimit)
    {
        if (m_data[itr.idxPrimary] == nullptr)
            return true;
        
        auto &vec = *m_data[itr.idxPrimary];
        for (; itr.idxSecondary < vec.size(); ++itr.idxSecondary)
        {
            // Assert we're ordered by T_MAX
            assert((itr.idxSecondary+1) >= vec.size() 
                || static_cast<T_MAX>(vec[itr.idxSecondary]) <= static_cast<T_MAX>(vec[itr.idxSecondary+1]));

            (*pcheckLimit)--;
            if (max < static_cast<T_MAX>(*itr))
                return *pcheckLimit > 0;

            size_t sizeBefore = vec.size();
            if (!fn(*itr))
            {
                itr.idxSecondary++; // we still visited this node
                return false;
            }
            if (vec.size() != sizeBefore)
            {
                assert(vec.size() == (sizeBefore-1));   // they may only remove the element passed to them
                --itr.idxSecondary;    // they deleted the element
            }
        }
        vec.shrink_to_fit();
        return *pcheckLimit > 0;
    }
};
