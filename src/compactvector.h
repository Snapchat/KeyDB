#pragma once

#include <type_traits>
#include <assert.h>

/*************************************************
 * compactvector - similar to std::vector but optimized for minimal memory
 *  
 *  Notable differences:
 *      - Limited to 2^32 elements
 *      - Grows linearly not exponentially
 * 
 *************************************************/

template<typename T, bool MEMMOVE_SAFE=false>
class compactvector
{
    static_assert(MEMMOVE_SAFE || std::is_trivially_copyable<T>::value, "compactvector requires trivially copyable types");
    T *m_data = nullptr;
    unsigned m_celem = 0;
    unsigned m_max = 0;

public:
    typedef T* iterator;

    compactvector() noexcept = default;
    ~compactvector() noexcept
    {
        clear();    // call dtors
        zfree(m_data);
    }

    compactvector(const compactvector &src)
    {
        m_celem = src.m_celem;
        m_max = src.m_max;
        m_data = (T*)zmalloc(sizeof(T) * m_max, MALLOC_LOCAL);
        for (size_t ielem = 0; ielem < m_celem; ++ielem)
        {
            new (m_data+ielem) T(src[ielem]);
        }
    }

    compactvector(compactvector &&src) noexcept
    {
        m_data = src.m_data;
        m_celem = src.m_celem;
        m_max = src.m_max;
        src.m_data = nullptr;
        src.m_celem = 0;
        src.m_max = 0;
    }

    compactvector &operator=(compactvector &&src) noexcept
    {
        zfree(m_data);
        m_data = src.m_data;
        m_celem = src.m_celem;
        m_max = src.m_max;
        src.m_data = nullptr;
        src.m_celem = 0;
        src.m_max = 0;
        return *this;
    }

    inline T* begin() { return m_data; }
    inline const T* begin() const { return m_data; }

    inline T* end() { return m_data + m_celem; }
    inline const T* end() const { return m_data + m_celem; }

    T* insert(T* where, const T &val)
    {
        assert(where >= m_data);
        size_t idx = where - m_data;
        if (m_celem >= m_max)
        {
            if (m_max < 2)
                m_max = 2;
            else
                m_max = m_max + 4;
            
            m_data = (T*)zrealloc(m_data, sizeof(T) * m_max, MALLOC_LOCAL);
            m_max = zmalloc_usable(m_data) / sizeof(T);
        }
        assert(idx < m_max);
        where = m_data + idx;
        memmove(reinterpret_cast<void*>(m_data + idx + 1), reinterpret_cast<const void*>(m_data + idx), (m_celem - idx)*sizeof(T));
        new(m_data + idx) T(std::move(val));
        ++m_celem;
        return where;
    }

    T &operator[](size_t idx)
    {
        assert(idx < m_celem);
        return m_data[idx];
    }
    const T &operator[](size_t idx) const
    {
        assert(idx < m_celem);
        return m_data[idx];
    }

    T& back() { assert(m_celem > 0); return m_data[m_celem-1]; }
    const T& back() const { assert(m_celem > 0); return m_data[m_celem-1]; }

    void erase(T* where)
    {
        assert(where >= m_data);
        size_t idx = where - m_data;
        assert(idx < m_celem);
        where->~T();
        memmove(reinterpret_cast<void*>(where), reinterpret_cast<const void*>(where+1), ((m_celem - idx - 1)*sizeof(T)));
        --m_celem;

        if (m_celem == 0)
        {
            zfree(m_data);
            m_data = nullptr;
            m_max = 0;
        }
    }

    void shrink_to_fit()
    {
        if (m_max == m_celem)
            return;
        m_data = (T*)zrealloc(m_data, sizeof(T) * m_celem, MALLOC_LOCAL);
        m_max = m_celem;    // NOTE: We do not get the usable size here, because this could cause us to continually realloc
    }

    size_t bytes_used() const
    {
        return sizeof(this) + (m_max * sizeof(T));
    }

    void clear()
    {
        for (size_t idx = 0; idx < m_celem; ++idx)
            m_data[idx].~T();
        zfree(m_data);
        m_data = nullptr;
        m_celem = 0;
        m_max = 0;
    }

    bool empty() const noexcept
    {
        return m_celem == 0;
    }

    size_t size() const noexcept
    {
        return m_celem;
    }

    T* data() noexcept { return m_data; }
    const T* data() const noexcept { return m_data; }
};
static_assert(sizeof(compactvector<void*>) <= 16, "not compact");
