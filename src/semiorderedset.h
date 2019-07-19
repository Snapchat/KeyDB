#pragma once
#include <assert.h>
#include "compactvector.h"

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

template<typename T, typename T_KEY = T>
class semiorderedset
{
    friend struct setiter;
    std::vector<compactvector<T>> m_data;
    size_t celem = 0;
    static const size_t bits_min = 8;
    size_t bits = bits_min;
    size_t idxRehash = (1ULL << bits_min); 
    bool fPauseRehash = false;

    constexpr size_t targetElementsPerBucket()
    {
        // Aim for roughly 2 cache lines per bucket (determined by imperical testing)
        //  lower values are faster but use more memory
        return std::max((64/sizeof(T))*2, (size_t)2);
    }

public:
    semiorderedset()
    {
        m_data.resize((1ULL << bits));
    }

    struct setiter
    {
        semiorderedset *set;
        size_t idxPrimary = 0;
        size_t idxSecondary = 0;

        setiter(semiorderedset *set)
        {
            this->set = set;
        }

        bool operator==(const setiter &other) const
        {
            return (idxPrimary == other.idxPrimary) && (idxSecondary == other.idxSecondary);
        }

        bool operator!=(const setiter &other) const { return !operator==(other); }

        inline T &operator*() { return set->m_data[idxPrimary][idxSecondary]; }
        inline const T &operator*() const { return set->m_data[idxPrimary][idxSecondary]; }

        inline T *operator->() { return &set->m_data[idxPrimary][idxSecondary]; }
        inline const T *operator->() const { return &set->m_data[idxPrimary][idxSecondary]; }
    };

    setiter find(const T_KEY &key)
    {
        RehashStep();
        setiter itr(this);
        itr.idxPrimary = idxFromObj(key);
 
        for (int hashset = 0; hashset < 2; ++hashset)   // rehashing may only be 1 resize behind, so we check up to two slots
        {
            auto &vecBucket = m_data[itr.idxPrimary];

            auto itrFind = std::find(vecBucket.begin(), vecBucket.end(), key);
            if (itrFind != vecBucket.end())
            {
                itr.idxSecondary =  itrFind - vecBucket.begin();
                return itr;
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

    setiter end()
    {
        setiter itr(this);
        itr.idxPrimary = m_data.size();
        return itr;
    }

    void insert(T &e, bool fRehash = false)
    {
        if (!fRehash)
            RehashStep();

        auto idx = idxFromObj(static_cast<T_KEY>(e));
        if (!fRehash)
            ++celem;
        
        typename compactvector<T>::iterator itrInsert;
        if (!m_data[idx].empty() && !(e < m_data[idx].back()))
            itrInsert = m_data[idx].end();
        else
            itrInsert = std::upper_bound(m_data[idx].begin(), m_data[idx].end(), e);
        itrInsert = m_data[idx].insert(itrInsert, e);

        if (celem > ((1ULL << bits)*targetElementsPerBucket()))
            grow();
    }

    // enumeration starting from the 'itrStart'th key.  Note that the iter is a hint, and need no be valid anymore
    template<typename T_VISITOR, typename T_MAX>
    setiter enumerate(const setiter &itrStart, const T_MAX &max, T_VISITOR fn)
    {
        setiter itr(itrStart);

        if (itrStart.set == this)   // really if this case isn't true its probably a bug
            itr = itrStart;         // but why crash the program when we can easily fix this?
        
        fPauseRehash = true;
        if (itr.idxPrimary >= m_data.size())
            itr.idxPrimary = 0;
        
        for (size_t ibucket = 0; ibucket < m_data.size(); ++ibucket)
        {
            if (!enumerate_bucket(itr, max, fn))
                break;
            itr.idxSecondary = 0;

            ++itr.idxPrimary;
            if (itr.idxPrimary >= m_data.size())
                itr.idxPrimary = 0;
        }
        fPauseRehash = false;
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
                if (idxSecondary < m_data[idxPrimary].size())
                {
                    ++visited;
                    fSawAny = true;
                    if (!fn(m_data[idxPrimary][idxSecondary]))
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
            if (m_data[idxPrimary].empty())
                continue;

            return m_data[idxPrimary][rand() % m_data[idxPrimary].size()];
        }
    }

    void erase(const setiter &itr)
    {
        auto &vecRow = m_data[itr.idxPrimary];
        vecRow.erase(vecRow.begin() + itr.idxSecondary);
        --celem;
        RehashStep();
    }

    void clear()
    {
        m_data = decltype(m_data)();
        bits = bits_min;
        m_data.resize(1ULL << bits);
        idxRehash = m_data.size();
    }
    
    bool empty() const noexcept { return celem == 0; }
    size_t size() const noexcept { return celem; }

    size_t bytes_used() const
    {
        size_t cb = sizeof(this) + (m_data.capacity()-m_data.size())*sizeof(T);
        for (auto &vec : m_data)
        {
            cb += vec.bytes_used();
        }
        return cb;
    }

private:
    inline size_t hashmask() const { return (1ULL << bits) - 1; }

    template<typename TT_KEY, typename std::enable_if_t<!std::is_pointer<TT_KEY>::value>* = nullptr>
    size_t idxFromObj(TT_KEY key)
    {
        static_assert(!std::is_pointer<TT_KEY>::value, "SFINAE isn't working");
        std::hash<TT_KEY> hash;
        return hash(key) & hashmask();
    }


    template<typename TT_KEY, typename std::enable_if_t<std::is_pointer<TT_KEY>::value>* = nullptr>
    size_t idxFromObj(TT_KEY key)
    {
        std::hash<TT_KEY> hash;
        size_t v = hash(key);
        // it's legal for std::hash to literally give us back the same pointer
        //  in which case the lower bits will have zero entropy.  Of course its also
        //  legal for std::hash to do what we're doing here in which case we're reducing
        //  lower order entropy... so rotate+XOR is the safest choice
        v ^= (v>>3) | (v << ((sizeof(v)*8)-3));
        return v & hashmask();
    }

    bool FRehashedRow(size_t idx) const
    {
        return (idx >= (m_data.size()/2)) || (idx < idxRehash);
    }

    void RehashStep()
    {
        if (fPauseRehash)
            return;
        
        int steps = 0;
        for (; idxRehash < (m_data.size()/2); ++idxRehash)
        {
            compactvector<T> vecT;
            std::swap(m_data[idxRehash], vecT); 

            for (auto &v : vecT)
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
    inline bool enumerate_bucket(setiter &itr, const T_MAX &max, T_VISITOR &fn)
    {
        auto &vec = m_data[itr.idxPrimary];
        for (; itr.idxSecondary < vec.size(); ++itr.idxSecondary)
        {
            // Assert we're ordered by T_MAX
            assert((itr.idxSecondary+1) >= vec.size() 
                || static_cast<T_MAX>(vec[itr.idxSecondary]) <= static_cast<T_MAX>(vec[itr.idxSecondary+1]));

            if (max < static_cast<T_MAX>(*itr))
                return true;

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
        return true;
    }
};
