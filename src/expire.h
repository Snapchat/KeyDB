#pragma once

#define INVALID_EXPIRE LLONG_MIN

class expireEntryFat
{
    friend class expireEntry;
    static const int INDEX_THRESHOLD = 16;
public:
    struct subexpireEntry
    {
        long long when;
        std::unique_ptr<const char, void(*)(const char*)> spsubkey;

        subexpireEntry(long long when, const char *subkey)
            : when(when), spsubkey(subkey, sdsfree)
        {}

        bool operator<(long long when) const noexcept { return this->when < when; }
        bool operator<(const subexpireEntry &se) { return this->when < se.when; }
    };

private:
    sds m_keyPrimary;
    std::vector<subexpireEntry> m_vecexpireEntries;  // Note a NULL for the sds portion means the expire is for the primary key
    dict *m_dictIndex = nullptr;

    void createIndex();
public:
    expireEntryFat(sds keyPrimary)
        : m_keyPrimary(keyPrimary)
        {}
    ~expireEntryFat();

    long long when() const noexcept { return m_vecexpireEntries.front().when; }
    long long whenFull() const noexcept {
        for (size_t i = 0; i < size(); ++i) {
            if (m_vecexpireEntries[i].spsubkey == nullptr) {
                return m_vecexpireEntries[i].when;
            }
        }
        return INVALID_EXPIRE;
    }
    const char *key() const noexcept { return m_keyPrimary; }

    bool operator<(long long when) const noexcept { return this->when() <  when; }

    void expireSubKey(const char *szSubkey, long long when);

    bool FEmpty() const noexcept { return m_vecexpireEntries.empty(); }
    const subexpireEntry &nextExpireEntry() const noexcept { return m_vecexpireEntries.front(); }
    void popfrontExpireEntry();
    const subexpireEntry &operator[](size_t idx) { return m_vecexpireEntries[idx]; }
    size_t size() const noexcept { return m_vecexpireEntries.size(); }
};

class expireEntry {
    union
    {
        sds m_key;
        expireEntryFat *m_pfatentry;
    } u;
    long long m_when;   // LLONG_MIN means this is a fat entry and we should use the pointer
    long long m_whenFull;
public:
    class iter
    {
        friend class expireEntry;
        expireEntry *m_pentry = nullptr;
        size_t m_idx = 0;

    public:
        iter(expireEntry *pentry, size_t idx)
            : m_pentry(pentry), m_idx(idx)
        {}

        iter &operator++() { ++m_idx; return *this; }
        
        const char *subkey() const
        {
            if (m_pentry->FFat())
                return (*m_pentry->pfatentry())[m_idx].spsubkey.get();
            return nullptr;
        }
        long long when() const
        {
            if (m_pentry->FFat())
                return (*m_pentry->pfatentry())[m_idx].when;
            return m_pentry->when();
        }

        bool operator!=(const iter &other)
        {
            return m_idx != other.m_idx;
        }

        const iter &operator*() const { return *this; }
    };

    expireEntry(sds key, const char *subkey, long long when)
    {
        if (subkey != nullptr)
        {
            m_when = INVALID_EXPIRE;
            m_whenFull = INVALID_EXPIRE;
            u.m_pfatentry = new (MALLOC_LOCAL) expireEntryFat(key);
            u.m_pfatentry->expireSubKey(subkey, when);
        }
        else
        {
            u.m_key = key;
            m_when = when;
            m_whenFull = when;
        }
    }

    expireEntry(expireEntryFat *pfatentry)
    {
        u.m_pfatentry = pfatentry;
        m_when = INVALID_EXPIRE;
        m_whenFull = pfatentry->whenFull();
    }

    expireEntry(expireEntry &&e)
    {
        u.m_key = e.u.m_key;
        m_when = e.m_when;
        m_whenFull = e.m_whenFull;
        e.u.m_key = (char*)key();  // we do this so it can still be found in the set
        e.m_when = 0;
    }

    ~expireEntry()
    {
        if (FFat())
            delete u.m_pfatentry;
    }

    void setKeyUnsafe(sds key)
    {
        if (FFat())
            u.m_pfatentry->m_keyPrimary = key;
        else
            u.m_key = key;
    }

    inline bool FFat() const noexcept { return m_when == INVALID_EXPIRE; }
    expireEntryFat *pfatentry() { assert(FFat()); return u.m_pfatentry; }


    bool operator==(const char *key) const noexcept
    { 
        return this->key() == key; 
    }

    bool operator<(const expireEntry &e) const noexcept
    { 
        return when() < e.when(); 
    }
    bool operator<(long long when) const noexcept
    { 
        return this->when() < when;
    }

    const char *key() const noexcept
    { 
        if (FFat())
            return u.m_pfatentry->key();
        return u.m_key;
    }
    long long when() const noexcept
    { 
        if (FFat())
            return u.m_pfatentry->when();
        return m_when; 
    }
    long long whenFull() const noexcept
    { 
        return m_whenFull; 
    }

    void update(const char *subkey, long long when)
    {
        if (subkey == nullptr)
        {
            m_whenFull = when;
        }
        if (!FFat())
        {
            if (subkey == nullptr)
            {
                m_when = when;
                return;
            }
            else
            {
                // we have to upgrade to a fat entry
                long long whenT = m_when;
                sds keyPrimary = u.m_key;
                m_when = INVALID_EXPIRE;
                u.m_pfatentry = new (MALLOC_LOCAL) expireEntryFat(keyPrimary);
                u.m_pfatentry->expireSubKey(nullptr, whenT);
                // at this point we're fat so fall through
            }
        }
        u.m_pfatentry->expireSubKey(subkey, when);
    }
    
    iter begin() { return iter(this, 0); }
    iter end()
    {
        if (FFat())
            return iter(this, u.m_pfatentry->size());
        return iter(this, 1);
    }
    
    void erase(iter &itr)
    {
        if (!FFat())
            throw -1;   // assert
        pfatentry()->m_vecexpireEntries.erase(
            pfatentry()->m_vecexpireEntries.begin() + itr.m_idx);
    }

    bool FGetPrimaryExpire(long long *pwhen)
    {
        *pwhen = -1;
        for (auto itr : *this)
        {
            if (itr.subkey() == nullptr)
            {
                *pwhen = itr.when();
                return true;
            }
        }
        return false;
    }

    explicit operator const char*() const noexcept { return key(); }
    explicit operator long long() const noexcept { return when(); }
};
typedef semiorderedset<expireEntry, const char *, true /*expireEntry can be memmoved*/> expireset;