#pragma once

template <class T>
class CowPtr
{
    public:
        typedef std::shared_ptr<T> RefPtr;

    private:
        mutable RefPtr m_sp;

        void detach()
        {
            if( !( m_sp == nullptr || m_sp.unique() ) ) {
                m_sp = std::make_shared<T>(*m_sp);
            }
        }

    public:
        CowPtr() = default;

        CowPtr(const RefPtr& refptr)
            :   m_sp(refptr)
        {
        }

        bool operator==(std::nullptr_t) const
        {
            return m_sp == nullptr;
        }

        bool operator!=(std::nullptr_t) const
        {
            return m_sp != nullptr;
        }
        
        const T& operator*() const
        {
            return *m_sp;
        }
        T& operator*()
        {
            detach();
            return *m_sp;
        }
        const T* operator->() const
        {
            return m_sp.operator->();
        }
        T* operator->()
        {
            detach();
            return m_sp.operator->();
        }
};
