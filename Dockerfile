# Multi-stage Dockerfile for KeyDB with Redis 8.2.3 Protocol Support
# Optimized for production use with TLS support

# ============================================================================
# Stage 1: Builder
# ============================================================================
FROM ubuntu:22.04 AS builder

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    $(dpkg --print-architecture | grep -q "amd64\|x86_64" && echo "nasm" || true) \
    autotools-dev \
    autoconf \
    libjemalloc-dev \
    tcl \
    tcl-dev \
    uuid-dev \
    libcurl4-openssl-dev \
    libbz2-dev \
    libzstd-dev \
    liblz4-dev \
    libsnappy-dev \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /keydb

# Copy source code
COPY . .

# Helper script for retrying commands that may fail under QEMU emulation
# GCC can segfault randomly under QEMU arm64; retrying usually succeeds
RUN printf '#!/bin/sh\nmax=8; attempt=1\nwhile [ $attempt -le $max ]; do\n  "$@" && exit 0\n  echo "Attempt $attempt/$max failed, cleaning corrupt objects and retrying..."\n  find . -name "*.o" -newer /usr/local/bin/retry -size -100c -delete 2>/dev/null || true\n  attempt=$((attempt+1)); sleep 1\ndone\necho "All $max attempts failed"; exit 1\n' > /usr/local/bin/retry && \
    chmod +x /usr/local/bin/retry

# Clean any previous builds and build dependencies
# ARM64 builds use -O0 (no optimization) and retry to handle QEMU GCC segfaults
RUN make clean || true && \
    if [ "$(uname -m)" = "aarch64" ]; then \
        cd deps && \
        CFLAGS="" retry make hiredis && \
        (cd jemalloc && [ -f Makefile ] && make distclean || true) && \
        CFLAGS="" retry make jemalloc JEMALLOC_CFLAGS="-std=gnu99 -Wall -pipe -g -O0" && \
        (cd lua && make clean || true) && \
        cd lua/src && CFLAGS="" retry make all CFLAGS="-O0 -Wall -DLUA_ANSI -DENABLE_CJSON_GLOBAL -DREDIS_STATIC='' -DLUA_USE_MKSTEMP" MYLDFLAGS="" AR="ar rc" && cd ../.. && \
        CFLAGS="" retry make hdr_histogram && \
        cd ..; \
    else \
        cd deps && \
        make hiredis && \
        (cd jemalloc && [ -f Makefile ] && make distclean || true) && \
        make jemalloc JEMALLOC_CFLAGS="-std=gnu99 -Wall -pipe -g -O2" && \
        make lua hdr_histogram -j$(nproc) && \
        cd ..; \
    fi

# Build KeyDB with TLS support
# ARM64: use -O0 (no optimization), single-threaded, with retry for QEMU stability
RUN if [ "$(uname -m)" = "aarch64" ]; then \
        retry make BUILD_TLS=yes OPTIMIZATION=-O0 -j1; \
    else \
        make BUILD_TLS=yes -j$(nproc); \
    fi

# ============================================================================
# Stage 2: Runtime
# ============================================================================
FROM ubuntu:22.04

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install gosu and runtime dependencies
ENV GOSU_VERSION=1.17
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        wget; \
    dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
    wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch"; \
    chmod +x /usr/local/bin/gosu; \
    gosu --version; \
    gosu nobody true; \
    apt-get install -y --no-install-recommends \
        libjemalloc2 \
        libcurl4 \
        libbz2-1.0 \
        libzstd1 \
        liblz4-1 \
        libsnappy1v5 \
        libssl3 \
        libuuid1 \
        tcl8.6; \
    apt-get purge -y --auto-remove wget; \
    rm -rf /var/lib/apt/lists/*

# Create keydb user and group
RUN groupadd -r -g 999 keydb && \
    useradd -r -g keydb -u 999 keydb

# Copy binaries from builder
COPY --from=builder /keydb/src/keydb-server /usr/local/bin/
COPY --from=builder /keydb/src/keydb-cli /usr/local/bin/
COPY --from=builder /keydb/src/keydb-benchmark /usr/local/bin/
COPY --from=builder /keydb/src/keydb-check-rdb /usr/local/bin/
COPY --from=builder /keydb/src/keydb-check-aof /usr/local/bin/
COPY --from=builder /keydb/src/keydb-sentinel /usr/local/bin/

# Create symlinks for redis compatibility
RUN ln -s /usr/local/bin/keydb-server /usr/local/bin/redis-server && \
    ln -s /usr/local/bin/keydb-cli /usr/local/bin/redis-cli && \
    ln -s /usr/local/bin/keydb-benchmark /usr/local/bin/redis-benchmark && \
    ln -s /usr/local/bin/keydb-check-rdb /usr/local/bin/redis-check-rdb && \
    ln -s /usr/local/bin/keydb-check-aof /usr/local/bin/redis-check-aof && \
    ln -s /usr/local/bin/keydb-sentinel /usr/local/bin/redis-sentinel

# Create directories
RUN mkdir -p /data /etc/keydb && \
    chown -R keydb:keydb /data /etc/keydb

# Copy default config
COPY keydb.conf /etc/keydb/keydb.conf
RUN chown keydb:keydb /etc/keydb/keydb.conf

# Create entrypoint script inline
RUN set -eux; \
    echo '#!/bin/sh' > /usr/local/bin/docker-entrypoint.sh; \
    echo 'set -e' >> /usr/local/bin/docker-entrypoint.sh; \
    echo '' >> /usr/local/bin/docker-entrypoint.sh; \
    echo '# Allow the container to be started with `--user`' >> /usr/local/bin/docker-entrypoint.sh; \
    echo 'if [ "$1" = "keydb-server" -a "$(id -u)" = "0" ]; then' >> /usr/local/bin/docker-entrypoint.sh; \
    echo '    find . \! -user keydb -exec chown keydb:keydb {} \;' >> /usr/local/bin/docker-entrypoint.sh; \
    echo '    exec gosu keydb "$0" "$@"' >> /usr/local/bin/docker-entrypoint.sh; \
    echo 'fi' >> /usr/local/bin/docker-entrypoint.sh; \
    echo '' >> /usr/local/bin/docker-entrypoint.sh; \
    echo '# Set password if KEYDB_PASSWORD is provided' >> /usr/local/bin/docker-entrypoint.sh; \
    echo 'if [ ! -z "${KEYDB_PASSWORD:-}" ]; then' >> /usr/local/bin/docker-entrypoint.sh; \
    echo '    echo "requirepass $KEYDB_PASSWORD" >> /etc/keydb/keydb.conf' >> /usr/local/bin/docker-entrypoint.sh; \
    echo 'fi' >> /usr/local/bin/docker-entrypoint.sh; \
    echo '' >> /usr/local/bin/docker-entrypoint.sh; \
    echo 'exec "$@"' >> /usr/local/bin/docker-entrypoint.sh; \
    chmod +x /usr/local/bin/docker-entrypoint.sh

# Set working directory
WORKDIR /data

# Expose ports
EXPOSE 6379

# Set volume
VOLUME ["/data"]

# Entrypoint (runs as root initially, then drops to keydb user via gosu)
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# Default command
CMD ["keydb-server", "/etc/keydb/keydb.conf"]

# Metadata
LABEL maintainer="Valerii Vainkop <vainkop@gmail.com>" \
      description="KeyDB with Redis 8.2.3 Protocol Support - Multi-master, Multithreaded, Kubernetes-ready" \
      version="8.2.3" \
      redis-protocol="8.2.3"

