#!/bin/bash

# make the build
git submodule init && git submodule update
make BUILD_TLS=yes ENABLE_FLASH=yes -j$(nproc) KEYDB_CFLAGS='-Werror' KEYDB_CXXFLAGS='-Werror'

# gen-cert
./utils/gen-test-certs.sh