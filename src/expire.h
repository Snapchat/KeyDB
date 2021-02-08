#pragma once

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

        subexpireEntry(const subexpireEntry &e)
            : when(e.when), spsubkey(nullptr, sdsfree)
        {
            if (e.spsubkey)
                spsubkey = std::unique_ptr<const char, void(*)(const char*)>((const char*)sdsdup((sds)e.spsubkey.get()), sdsfree);
        }

        subexpireEntry(subexpireEntry &&e) = default;
        subexpireEntry& operator=(subexpireEntry &&e) = default;

        bool operator<(long long when) const noexcept { return this->when < when; }
        bool operator<(const subexpireEntry &se) { return this->when < se.when; }
    };

private:
    sdsimmutablestring m_keyPrimary;
    std::vector<subexpireEntry> m_vecexpireEntries;  // Note a NULL for the sds portion means the expire is for the primary key
    dict *m_dictIndex = nullptr;

    void createIndex();
public:
    expireEntryFat(const sdsimmutablestring &keyPrimary)
        : m_keyPrimary(keyPrimary)
        {}
    ~expireEntryFat();

    expireEntryFat(const expireEntryFat &e);
    expireEntryFat(expireEntryFat &&e);

    long long when() const noexcept { return m_vecexpireEntries.front().when; }
    const char *key() const noexcept { return static_cast<const char*>(m_keyPrimary); }

    bool operator<(long long when) const noexcept { return this->when() <  when; }

    void expireSubKey(const char *szSubkey, long long when);

    bool FEmpty() const noexcept { return m_vecexpireEntries.empty(); }
    const subexpireEntry &nextExpireEntry() const noexcept { return m_vecexpireEntries.front(); }
    void popfrontExpireEntry();
    const subexpireEntry &operator[](size_t idx) const { return m_vecexpireEntries[idx]; }
    size_t size() const noexcept { return m_vecexpireEntries.size(); }
};

class expireEntry {
    struct
    {
        sdsimmutablestring m_key;
        expireEntryFat *m_pfatentry = nullptr;
    } u;
    long long m_when;   // LLONG_MIN means this is a fat entry and we should use the pointer

public:
    class iter
    {
        friend class expireEntry;
        const expireEntry *m_pentry = nullptr;
        size_t m_idx = 0;

    public:
        iter(const expireEntry *pentry, size_t idx)
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
            m_when = LLONG_MIN;
            u.m_pfatentry = new (MALLOC_LOCAL) expireEntryFat(sdsimmutablestring(sdsdupshared(key)));
            u.m_pfatentry->expireSubKey(subkey, when);
        }
        else
        {
            u.m_key = sdsimmutablestring(sdsdupshared(key));
            m_when = when;
        }
    }

    expireEntry(const expireEntry &e)
    {
        *this = e;
    }
    expireEntry(expireEntry &&e)
    {
        u.m_key = std::move(e.u.m_key);
        u.m_pfatentry = std::move(e.u.m_pfatentry);
        m_when = e.m_when;
        e.m_when = 0;
        e.u.m_pfatentry = nullptr;
    }

    expireEntry(expireEntryFat *pfatentry)
    {
        u.m_pfatentry = pfatentry;
        m_when = LLONG_MIN;
    }

    ~expireEntry()
    {
        if (FFat())
            delete u.m_pfatentry;
    }

    expireEntry &operator=(const expireEntry &e)
    {
        u.m_key = e.u.m_key;
        m_when = e.m_when;
        if (e.FFat())
            u.m_pfatentry = new (MALLOC_LOCAL) expireEntryFat(*e.u.m_pfatentry);
        return *this;
    }

    void setKeyUnsafe(sds key)
    {
        if (FFat())
            u.m_pfatentry->m_keyPrimary = sdsimmutablestring(sdsdupshared(key));
        else
            u.m_key = sdsimmutablestring(sdsdupshared(key));
    }

    inline bool FFat() const noexcept { return m_when == LLONG_MIN; }
    expireEntryFat *pfatentry() { assert(FFat()); return u.m_pfatentry; }
    const expireEntryFat *pfatentry() const { assert(FFat()); return u.m_pfatentry; }


    bool operator==(const sdsview &key) const noexcept
    { 
        return key == this->key(); 
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
        return static_cast<const char*>(u.m_key);
    }
    long long when() const noexcept
    { 
        if (FFat())
            return u.m_pfatentry->when();
        return m_when; 
    }

    void update(const char *subkey, long long when)
    {
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
                sdsimmutablestring keyPrimary = u.m_key;
                m_when = LLONG_MIN;
                u.m_pfatentry = new (MALLOC_LOCAL) expireEntryFat(keyPrimary);
                u.m_pfatentry->expireSubKey(nullptr, whenT);
                // at this point we're fat so fall through
            }
        }
        u.m_pfatentry->expireSubKey(subkey, when);
    }
    
    iter begin() const { return iter(this, 0); }
    iter end() const
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

    size_t size() const
    {
        if (FFat())
            return u.m_pfatentry->size();
        return 1;
    }

    bool FGetPrimaryExpire(long long *pwhen) const
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

    explicit operator sdsview() const noexcept { return key(); }
    explicit operator long long() const noexcept { return when(); }
};
typedef semiorderedset<expireEntry, sdsview, true /*expireEntry can be memmoved*/> expireset;
extern fastlock g_expireLock;