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
            : spsubkey(nullptr, sdsfree)
        {
            when = other.when;
            if (other.spsubkey != nullptr)
                spsubkey = std::unique_ptr<const char, void(*)(const char*)>((const char*)sdsdupshared(other.spsubkey.get()), sdsfree);
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
    std::vector<subexpireEntry> m_vecexpireEntries;  // Note a NULL for the sds portion means the expire is for the primary key
    dict *m_dictIndex = nullptr;
    long long m_whenPrimary = LLONG_MAX;

    void createIndex();
public:
    expireEntryFat() = default;
    expireEntryFat(const expireEntryFat &);
    ~expireEntryFat();

    long long when() const noexcept { return m_vecexpireEntries.front().when; }

    bool operator<(long long when) const noexcept { return this->when() <  when; }

    void expireSubKey(const char *szSubkey, long long when);

    bool FGetPrimaryExpire(long long *pwhen) const {
        if (m_whenPrimary != LLONG_MAX) {
            *pwhen = m_whenPrimary;
            return true;
        }
        return false;
    }

    bool FEmpty() const noexcept { return m_vecexpireEntries.empty(); }
    const subexpireEntry &nextExpireEntry() const noexcept { return m_vecexpireEntries.front(); }
    void popfrontExpireEntry();
    const subexpireEntry &operator[](size_t idx) const { return m_vecexpireEntries[idx]; }
    size_t size() const noexcept { return m_vecexpireEntries.size(); }
};

class expireEntry {
    struct {
        uint64_t m_whenAndPtrUnion : 63,
                fFat : 1;
    } s;
    static_assert(sizeof(expireEntryFat*) <= sizeof(int64_t), "The pointer must fit in the union");
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

    expireEntry() 
    {
        s.fFat = 0;
        s.m_whenAndPtrUnion = 0;
    }

    expireEntry(const char *subkey, long long when)
    {
        if (subkey != nullptr)
        {
            auto pfatentry = new (MALLOC_LOCAL) expireEntryFat();
            pfatentry->expireSubKey(subkey, when);
            s.m_whenAndPtrUnion = reinterpret_cast<long long>(pfatentry);
            s.fFat = true;
        }
        else
        {
            s.m_whenAndPtrUnion = when;
            s.fFat = false;
        }
    }

    expireEntry(expireEntryFat *pfatentry)
    {
        assert(pfatentry != nullptr);
        s.m_whenAndPtrUnion = reinterpret_cast<long long>(pfatentry);
        s.fFat = true;
    }

    expireEntry(const expireEntry &e) {
        if (e.FFat()) {
            s.m_whenAndPtrUnion = reinterpret_cast<long long>(new expireEntryFat(*e.pfatentry()));
            s.fFat = true;
        } else {
            s = e.s;
        }
    }

    expireEntry(expireEntry &&e)
    {
        s = e.s;
    }

    expireEntry &operator=(expireEntry &&e)
    {
        if (FFat())
            delete pfatentry();
        s = e.s;
        e.s.m_whenAndPtrUnion = 0;
        e.s.fFat = false;
        return *this;
    }

    expireEntry &operator=(expireEntry &e) {
        if (FFat())
            delete pfatentry();
        if (e.FFat()) {
            s.m_whenAndPtrUnion = reinterpret_cast<long long>(new expireEntryFat(*e.pfatentry()));
            s.fFat = true;
        } else {
            s = e.s;
        }
        return *this;
    }

    // Duplicate the expire, note this is intended to be passed directly to setExpire
    expireEntry duplicate() const {
        expireEntry dst;
        if (FFat()) {
            auto pfatentry = new expireEntryFat(*expireEntry::pfatentry());
            dst.s.m_whenAndPtrUnion = reinterpret_cast<long long>(pfatentry);
            dst.s.fFat = true;
        } else {
            dst.s.m_whenAndPtrUnion = s.m_whenAndPtrUnion;
            dst.s.fFat = false;
        }
        return dst;
    }

    void reset() {
        if (FFat())
            delete pfatentry();
        s.fFat = false;
        s.m_whenAndPtrUnion = 0;
    }

    ~expireEntry()
    {
        if (FFat())
            delete pfatentry();
    }

    inline bool FFat() const noexcept { return s.fFat; }
    expireEntryFat *pfatentry() { 
        assert(FFat()); 
        return reinterpret_cast<expireEntryFat*>(s.m_whenAndPtrUnion);
    }
    const expireEntryFat *pfatentry() const { 
        return const_cast<expireEntry*>(this)->pfatentry();
    }


    bool operator<(const expireEntry &e) const noexcept
    { 
        return when() < e.when(); 
    }
    bool operator<(long long when) const noexcept
    { 
        return this->when() < when;
    }

    long long when() const noexcept
    { 
        if (FFat())
            return pfatentry()->when();
        return s.m_whenAndPtrUnion;
    }

    void update(const char *subkey, long long when)
    {
        if (!FFat())
        {
            if (subkey == nullptr)
            {
                s.m_whenAndPtrUnion = when;
                return;
            }
            else
            {
                // we have to upgrade to a fat entry
                auto pfatentry = new (MALLOC_LOCAL) expireEntryFat();
                pfatentry->expireSubKey(nullptr, s.m_whenAndPtrUnion);
                s.m_whenAndPtrUnion = reinterpret_cast<long long>(pfatentry);
                s.fFat = true;
                // at this point we're fat so fall through
            }
        }
        pfatentry()->expireSubKey(subkey, when);
    }
    
    iter begin() const { return iter(this, 0); }
    iter end() const
    {
        if (FFat())
            return iter(this, pfatentry()->size());
        return iter(this, 1);
    }
    
    void erase(iter &itr)
    {
        if (!FFat())
            throw -1;   // assert
        pfatentry()->m_vecexpireEntries.erase(
            pfatentry()->m_vecexpireEntries.begin() + itr.m_idx);
    }

    size_t size() const {
        if (FFat())
            return pfatentry()->size();
        return 1;
    }

    bool FGetPrimaryExpire(long long *pwhen) const noexcept
    { 
        if (FFat()) {
            return pfatentry()->FGetPrimaryExpire(pwhen);
        } else {
            *pwhen = s.m_whenAndPtrUnion;
            return true;
        }
    }

    void *release_as_void() {
        uint64_t whenT = s.m_whenAndPtrUnion;
        whenT |= static_cast<uint64_t>(s.fFat) << 63;
        s.m_whenAndPtrUnion = 0;
        s.fFat = 0;
        return reinterpret_cast<void*>(whenT);
    }

    static expireEntry *from_void(void **src) {
        uintptr_t llV = reinterpret_cast<uintptr_t>(src);
        return reinterpret_cast<expireEntry*>(llV);
    }
    static const expireEntry *from_void(void *const*src) {
        uintptr_t llV = reinterpret_cast<uintptr_t>(src);
        return reinterpret_cast<expireEntry*>(llV);
    }

    explicit operator long long() const noexcept { return when(); }
};
static_assert(sizeof(expireEntry) == sizeof(long long), "This must fit in a long long so it can be put in a dictEntry");
