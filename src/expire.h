#pragma once

/* 
 * INVALID_EXPIRE is the value we set the expireEntry's m_when value to when the main key is not expired and the value we return when we try to get the expire time of a key or subkey that is not expired
 * Want this value to be LLONG_MAX however we use the most significant bit of m_when as a flag to see if the expireEntry is Fat or not so we want to ensure that it is unset hence the (& ~((1LL) << (sizeof(long long)*CHAR_BIT - 1)))
 * Eventually this number might actually end up being a valid expire time, this could cause bugs so at that time it might be a good idea to use a larger data type.
 */
#define INVALID_EXPIRE (LLONG_MAX & ~((1LL) << (sizeof(long long)*CHAR_BIT - 1)))

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

        subexpireEntry(const subexpireEntry &other)
            : spsubkey((const char*)sdsdupshared(other.spsubkey.get()), sdsfree)
        {
            when = other.when;
        }

        subexpireEntry(subexpireEntry &&) = default;
        subexpireEntry& operator=(subexpireEntry&&) = default;

        subexpireEntry& operator=(const subexpireEntry &src) {
            when = src.when;
            spsubkey = std::unique_ptr<const char, void(*)(const char*)>((const char*)sdsdupshared(src.spsubkey.get()), sdsfree);
            return *this;
        }

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
    long long m_when;   // bit wise and with FFatMask means this is a fat entry and we should use the pointer

    /* Mask to check if an entry is Fat, most significant bit of m_when being set means it is Fat otherwise it is not */
    long long FFatMask() const noexcept {
        return (1LL) << (sizeof(long long)*CHAR_BIT - 1);
    }

    expireEntry() = default;
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
            m_when = FFatMask() | INVALID_EXPIRE;
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
        m_when = FFatMask() | INVALID_EXPIRE;
        for (auto itr : *this)
        {
            if (itr.subkey() == nullptr)
            {
                m_when = FFatMask() | itr.when();
                break;
            }
        }
    }

    // Duplicate the expire, note this is intended to be passed directly to setExpire
    expireEntry duplicate() const {
        expireEntry dst;
        dst.m_when = m_when;
        if (FFat()) {
            dst.u.m_pfatentry = new expireEntryFat(*u.m_pfatentry);
        } else {
            dst.u.m_key = u.m_key;
        }
        return dst;
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

    inline bool FFat() const noexcept { return m_when & FFatMask(); }
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
        return FGetPrimaryExpire();
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
                m_when |= FFatMask();
                u.m_pfatentry = new (MALLOC_LOCAL) expireEntryFat(keyPrimary);
                u.m_pfatentry->expireSubKey(nullptr, whenT);
                // at this point we're fat so fall through
            }
        }
        if (subkey == nullptr)
            m_when = when | FFatMask();
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

    long long FGetPrimaryExpire() const noexcept
    { 
        return m_when & (~FFatMask()); 
    }

    bool FGetPrimaryExpire(long long *pwhen) const noexcept
    { 
        *pwhen = FGetPrimaryExpire();
        return *pwhen != INVALID_EXPIRE;
    }

    explicit operator sdsview() const noexcept { return key(); }
    explicit operator long long() const noexcept { return when(); }
};
typedef semiorderedset<expireEntry, sdsview, true /*expireEntry can be memmoved*/> expireset;
extern fastlock g_expireLock;