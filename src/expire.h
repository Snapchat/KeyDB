#pragma once

#define INVALID_EXPIRE LLONG_MIN
#define MAINKEYSTRING ""

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
        bool operator<(const subexpireEntry &se) const noexcept { return this->when < se.when; }
        bool operator==(sdsview subkey) const noexcept { return sdsview((sds)spsubkey.get()) == subkey; }

        explicit operator sdsview() const noexcept { return sdsview((sds)spsubkey.get()); }
        explicit operator long long() const noexcept { return when; }
    };
    class subexpiresethashobj {
    public:
        size_t operator()(const sdsview &key)
        {
            return (size_t)dictGenHashFunction(key.get(), sdslen(key.get()));;
        }
    };
    typedef semiorderedset<subexpireEntry, sdsview, true /*subexpireEntry can be memmoved*/, subexpiresethashobj> subexpireset;

private:
    sds m_keyPrimary;
    subexpireset m_subexpireEntries;  // Note a NULL for the sds portion means the expire is for the primary key

public:
    expireEntryFat(sds keyPrimary)
        : m_keyPrimary(keyPrimary)
        {}
    ~expireEntryFat() {}

    const char *key() const noexcept { return m_keyPrimary; }

    void expireSubKey(const char *szSubkey, long long when);

    bool FEmpty() const noexcept { return m_subexpireEntries.empty(); }
    const subexpireEntry &nextExpireEntry() const noexcept { return m_subexpireEntries.random_value_finite(); }
    void popExpireEntry(const char *szSubkey);
    size_t size() const noexcept { return m_subexpireEntries.size(); }
    subexpireset::setiter find(const char *szSubkey) { sds mainkey = sdsnew(MAINKEYSTRING); sdsview subkey(szSubkey ? szSubkey : mainkey); return m_subexpireEntries.find(subkey); sdsfree(mainkey); }
    subexpireset::setiter end() { return m_subexpireEntries.end(); }
    void erase(subexpireset::setiter itr) { m_subexpireEntries.erase(itr); }
    template<typename T_VISITOR, typename T_MAX>
    subexpireset::setiter enumerate(const subexpireset::setiter &itrStart, const T_MAX &max, T_VISITOR fn, long long *pccheck) 
    {
        return m_subexpireEntries.enumerate(itrStart, max, fn, pccheck);
    }
    template<typename T_VISITOR>
    size_t random_visit(T_VISITOR fn) { return m_subexpireEntries.random_visit(fn); }
};

class expireEntry {
    union
    {
        sds m_key;
        expireEntryFat *m_pfatentry;
    } u;
    long long m_when;   // LLONG_MIN means this is a fat entry and we should use the pointer

    long long FFatMask() const noexcept {
        return (1LL) << (sizeof(long long)*CHAR_BIT - 1);
    }

public:
    class iter {
        friend class expireEntry;
        expireEntry *m_pentry = nullptr;
        expireEntryFat::subexpireset::setiter m_iter;

    public:
        iter(expireEntry *pentry, expireEntryFat::subexpireset::setiter iter) 
            : m_pentry(pentry), m_iter(iter)
        {}

        const char *subkey() const
        {
            if (m_pentry->FFat())
                return m_iter->spsubkey.get();
            return nullptr;
        }
        long long when() const
        {
            if (m_pentry->FFat())
                return m_iter->when;
            return m_pentry->when();
        }

        bool operator!=(const iter &other)
        {
            return m_iter != other.m_iter;
        }

        const iter &operator*() const { return *this; }

        const expireEntryFat::subexpireset::setiter &setiter() { return m_iter; }
    };

    expireEntry(sds key, const char *subkey, long long when)
    {
        if (subkey != nullptr)
        {
            m_when = when | FFatMask();
            u.m_pfatentry = new (MALLOC_LOCAL) expireEntryFat(key);
            u.m_pfatentry->expireSubKey(subkey, when);
        }
        else
        {
            u.m_key = key;
            m_when = when;
        }
    }

    expireEntry(expireEntryFat *pfatentry)
    {
        u.m_pfatentry = pfatentry;
        m_when = FFatMask();
    }

    expireEntry(expireEntry &&e)
    {
        u.m_key = e.u.m_key;
        m_when = e.m_when;
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

    inline bool FFat() const noexcept { return m_when & FFatMask(); }
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
        return m_when & (~FFatMask()); 
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
                long long whenT = this->when();
                sds keyPrimary = u.m_key;
                m_when |= FFatMask();
                u.m_pfatentry = new (MALLOC_LOCAL) expireEntryFat(keyPrimary);
                u.m_pfatentry->expireSubKey(nullptr, whenT);
                // at this point we're fat so fall through
            }
        }
        if (this->when() > when) {
            m_when = when | FFatMask();
        }
        u.m_pfatentry->expireSubKey(subkey, when);
    }

    bool FGetPrimaryExpire(long long *pwhen)
    {
        *pwhen = -1;
        if (FFat()) {
            auto itr = u.m_pfatentry->find(nullptr);
            if (itr != u.m_pfatentry->end()) {
                *pwhen = itr->when;
                return true;
            }
            else {
                return false;
            }
        }
        else {
            *pwhen = m_when;
            return true;
        }
    }

    iter find(const char *subkey) {
        if (FFat())
            return iter(this,u.m_pfatentry->find(subkey));
        else if (subkey == nullptr) {
            return iter(this,expireEntryFat::subexpireset::setiter(nullptr));
        }
        return end();
    }

    iter end() {
        if (FFat())
            return iter(this,u.m_pfatentry->end());
        expireEntryFat::subexpireset::setiter enditer(nullptr);
        enditer.idxPrimary = 1;
        return iter(this,enditer);
    }

    template<typename T_VISITOR, typename T_MAX>
    expireEntryFat::subexpireset::setiter enumerate(const expireEntryFat::subexpireset::setiter &itrStart, const T_MAX &max, T_VISITOR fn, long long *pccheck) 
    {
        if (FFat())
            return u.m_pfatentry->enumerate(itrStart, max, fn, pccheck);
        expireEntryFat::subexpireEntry se(m_when, sdsnew(MAINKEYSTRING));
        fn(se);
        return end().setiter();
    }

    explicit operator const char*() const noexcept { return key(); }
    explicit operator long long() const noexcept { return when(); }
};
typedef semiorderedset<expireEntry, const char *, true /*expireEntry can be memmoved*/> expireset;