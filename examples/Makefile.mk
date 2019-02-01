#
#  Copyright (C) 2014 - 2018 Intel Corporation.
#  All rights reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#  1. Redistributions of source code must retain the above copyright notice(s),
#     this list of conditions and the following disclaimer.
#  2. Redistributions in binary form must reproduce the above copyright notice(s),
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER(S) ``AS IS'' AND ANY EXPRESS
#  OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
#  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
#  EVENT SHALL THE COPYRIGHT HOLDER(S) BE LIABLE FOR ANY DIRECT, INDIRECT,
#  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
#  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
#  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
#  OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
#  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

noinst_PROGRAMS += examples/hello_memkind \
                   examples/hello_memkind_debug \
                   examples/hello_hbw \
                   examples/filter_memkind \
                   examples/pmem_kinds \
                   examples/pmem_malloc \
                   examples/pmem_malloc_unlimited \
                   examples/pmem_usable_size \
                   examples/pmem_alignment \
                   examples/pmem_and_default_kind \
                   examples/pmem_multithreads \
                   examples/pmem_multithreads_onekind \
                   examples/pmem_free_with_unknown_kind \
                   examples/autohbw_candidates \
                   # end
if HAVE_CXX11
noinst_PROGRAMS += examples/memkind_allocated
noinst_PROGRAMS += examples/pmem_cpp_allocator
endif

examples_hello_memkind_LDADD = libmemkind.la
examples_hello_memkind_debug_LDADD = libmemkind.la
examples_hello_hbw_LDADD = libmemkind.la
examples_filter_memkind_LDADD = libmemkind.la
examples_pmem_kinds_LDADD = libmemkind.la
examples_pmem_malloc_LDADD = libmemkind.la
examples_pmem_malloc_unlimited_LDADD = libmemkind.la
examples_pmem_usable_size_LDADD = libmemkind.la
examples_pmem_alignment_LDADD = libmemkind.la
examples_pmem_and_default_kind_LDADD = libmemkind.la
examples_pmem_multithreads_LDADD = libmemkind.la
examples_pmem_multithreads_onekind_LDADD = libmemkind.la
examples_pmem_free_with_unknown_kind_LDADD = libmemkind.la
examples_autohbw_candidates_LDADD = libmemkind.la

if HAVE_CXX11
examples_memkind_allocated_LDADD = libmemkind.la
examples_pmem_cpp_allocator_LDADD = libmemkind.la
endif

examples_hello_memkind_SOURCES = examples/hello_memkind_example.c
examples_hello_memkind_debug_SOURCES = examples/hello_memkind_example.c examples/memkind_decorator_debug.c
examples_hello_hbw_SOURCES = examples/hello_hbw_example.c
examples_filter_memkind_SOURCES = examples/filter_example.c
examples_pmem_kinds_SOURCES = examples/pmem_kinds.c
examples_pmem_malloc_SOURCES = examples/pmem_malloc.c
examples_pmem_malloc_unlimited_SOURCES = examples/pmem_malloc_unlimited.c
examples_pmem_usable_size_SOURCES = examples/pmem_usable_size.c
examples_pmem_alignment_SOURCES = examples/pmem_alignment.c
examples_pmem_and_default_kind_SOURCES = examples/pmem_and_default_kind.c
examples_pmem_multithreads_SOURCES = examples/pmem_multithreads.c
examples_pmem_multithreads_onekind_SOURCES = examples/pmem_multithreads_onekind.c
examples_pmem_free_with_unknown_kind_SOURCES = examples/pmem_free_with_unknown_kind.c
examples_autohbw_candidates_SOURCES = examples/autohbw_candidates.c
if HAVE_CXX11
examples_memkind_allocated_SOURCES = examples/memkind_allocated_example.cpp examples/memkind_allocated.hpp
examples_pmem_cpp_allocator_SOURCES = examples/pmem_cpp_allocator.cpp
endif

clean-local:
	rm -f examples/*.gcno examples/*.gcda
