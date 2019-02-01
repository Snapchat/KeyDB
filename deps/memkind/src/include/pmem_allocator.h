/*
 * Copyright (C) 2018 Intel Corporation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice(s),
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice(s),
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER(S) ``AS IS'' AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER(S) BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once

#include <memory>
#include <string>
#include <exception>
#include <type_traits>
#include <atomic>
#include <cstddef>

#include "memkind.h"

/*
 * Header file for the C++ allocator compatible with the C++ standard library allocator concepts.
 * More details in pmemallocator(3) man page.
 * Note: memory heap management is based on memkind_malloc, refer to the memkind(3) man page for more
 * information.
 *
 * Functionality defined in this header is considered as EXPERIMENTAL API.
 * API standards are described in memkind(3) man page.
 */
namespace pmem
{
    namespace internal
    {
        class kind_wrapper_t
        {
        public:
            kind_wrapper_t(const char *dir, std::size_t max_size)
            {
                int err_c  = memkind_create_pmem(dir, max_size, &kind);
                if (err_c) {
                    throw std::runtime_error(
                        std::string("An error occured while creating pmem kind; error code: ") +
                        std::to_string(err_c));
                }
            }

            kind_wrapper_t(const kind_wrapper_t &) = delete;
            void operator=(const kind_wrapper_t &) = delete;

            ~kind_wrapper_t()
            {
                memkind_destroy_kind(kind);
            }

            memkind_t get() const
            {
                return kind;
            }

        private:
            memkind_t kind;
        };
    }

    template<typename T>
    class allocator
    {
        using kind_wrapper_t = internal::kind_wrapper_t;
        std::shared_ptr<kind_wrapper_t> kind_wrapper_ptr;
    public:
        using value_type = T;
        using pointer = value_type*;
        using const_pointer = const value_type*;
        using reference = value_type&;
        using const_reference = const value_type&;
        using size_type = size_t;
        using difference_type = ptrdiff_t;

        template<class U>
        struct rebind {
            using other = allocator<U>;
        };

        template<typename U>
        friend class allocator;

#ifndef _GLIBCXX_USE_CXX11_ABI
        /* This is a workaround for compilers (e.g GCC 4.8) that uses C++11 standard,
         * but use old - non C++11 ABI */
        template<typename V = void>
        explicit allocator()
        {
            static_assert(std::is_same<V, void>::value,
                          "pmem::allocator cannot be compiled without CXX11 ABI");
        }
#endif

        explicit allocator(const char *dir, size_t max_size) :
            kind_wrapper_ptr(std::make_shared<kind_wrapper_t>(dir, max_size))
        {
        }

        explicit allocator(const std::string &dir, size_t max_size) :
            allocator(dir.c_str(), max_size)
        {
        }

        allocator(const allocator &other) = default;

        template <typename U>
        allocator(const allocator<U> &other) noexcept : kind_wrapper_ptr(
                other.kind_wrapper_ptr)
        {
        }

        allocator(allocator &&other) = default;

        template <typename U>
        allocator(allocator<U> &&other) noexcept :
            kind_wrapper_ptr(std::move(other.kind_wrapper_ptr))
        {
        }

        allocator<T> &operator = (const allocator &other) = default;

        template <typename U>
        allocator<T> &operator = (const allocator<U> &other) noexcept
        {
            kind_wrapper_ptr = other.kind_wrapper_ptr;
            return *this;
        }

        allocator<T> &operator = (allocator &&other) = default;

        template <typename U>
        allocator<T> &operator = (allocator<U> &&other) noexcept
        {
            kind_wrapper_ptr = std::move(other.kind_wrapper_ptr);
            return *this;
        }

        pointer allocate(size_type n) const
        {
            pointer result = static_cast<pointer>(memkind_malloc(kind_wrapper_ptr->get(),
                                                                 n*sizeof(T)));
            if (!result) {
                throw std::bad_alloc();
            }
            return result;
        }

        void deallocate(pointer p, size_type n) const
        {
            memkind_free(kind_wrapper_ptr->get(), static_cast<void *>(p));
        }

        template <class U, class... Args>
        void construct(U *p, Args &&... args) const
        {
            ::new((void *)p) U(std::forward<Args>(args)...);
        }

        void destroy(pointer p) const
        {
            p->~value_type();
        }

        template <typename U, typename V>
        friend bool operator ==(const allocator<U> &lhs, const allocator<V> &rhs);

        template <typename U, typename V>
        friend bool operator !=(const allocator<U> &lhs, const allocator<V> &rhs);
    };

    template <typename U, typename V>
    bool operator ==(const allocator<U> &lhs, const allocator<V> &rhs)
    {
        return lhs.kind_wrapper_ptr->get() == rhs.kind_wrapper_ptr->get();
    }

    template <typename U, typename V>
    bool operator !=(const allocator<U> &lhs, const allocator<V> &rhs)
    {
        return !(lhs == rhs);
    }
}
