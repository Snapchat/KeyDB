FROM ubuntu:20.04
SHELL ["/bin/bash","-c"]
RUN groupadd -r keydb && useradd -r -g keydb keydb
# use gosu for easy step-down from root: https://github.com/tianon/gosu/releases
ENV GOSU_VERSION 1.14
RUN set -eux; \
        savedAptMark="$(apt-mark showmanual)"; \
        apt-get update; \
        apt-get install -y --no-install-recommends ca-certificates dirmngr gnupg wget; \
        rm -rf /var/lib/apt/lists/*; \
        dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
        wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch"; \
        wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch.asc"; \
        export GNUPGHOME="$(mktemp -d)"; \
        gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4; \
        gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
        gpgconf --kill all; \
        rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
        apt-mark auto '.*' > /dev/null; \
        [ -z "$savedAptMark" ] || apt-mark manual $savedAptMark > /dev/null; \
        apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
        chmod +x /usr/local/bin/gosu; \
        gosu --version; \
        gosu nobody true
# build KeyDB
ARG BRANCH
RUN set -eux; \
        \
        savedAptMark="$(apt-mark showmanual)"; \
        apt-get update; \
        DEBIAN_FRONTEND=noninteractive apt-get install -qqy --no-install-recommends \
                dpkg-dev \
                pkg-config \
                ca-certificates \
                build-essential \
                nasm \
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
                git; \
        cd /tmp && git clone --branch $BRANCH https://github.com/Snapchat/KeyDB.git --recursive; \
        cd /tmp/KeyDB; \
        # disable protected mode as it relates to docker
        grep -E '^ *createBoolConfig[(]"protected-mode",.*, *1 *,.*[)],$' ./src/config.cpp; \
        sed -ri 's!^( *createBoolConfig[(]"protected-mode",.*, *)1( *,.*[)],)$!\10\2!' ./src/config.cpp; \
        grep -E '^ *createBoolConfig[(]"protected-mode",.*, *0 *,.*[)],$' ./src/config.cpp; \
        make -j$(nproc) BUILD_TLS=yes; \
        cd src; \
        strip keydb-cli keydb-benchmark keydb-check-rdb keydb-check-aof keydb-diagnostic-tool keydb-sentinel keydb-server; \
        mv keydb-server keydb-cli keydb-benchmark keydb-check-rdb keydb-check-aof keydb-diagnostic-tool keydb-sentinel /usr/local/bin/; \
        # clean up unused dependencies
        echo $savedAptMark; \
        apt-mark auto '.*' > /dev/null; \
        [ -z "$savedAptMark" ] || apt-mark manual $savedAptMark > /dev/null; \
        find /usr/local -type f -executable -exec ldd '{}' ';' \
               | awk '/=>/ { print $(NF-1) }' \
               | sed 's:.*/::' \
               | sort -u \
               | xargs -r dpkg-query --search \
               | cut -d: -f1 \
               | sort -u \
               | xargs -r apt-mark manual \
        ; \
        apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
        rm -rf /var/lib/apt/lists/*; \
# create working directories and organize files
RUN \
        mkdir /data && chown keydb:keydb /data; \
        mkdir /flash && chown keydb:keydb /flash; \
        mkdir -p /etc/keydb; \
        cp /tmp/KeyDB/keydb.conf /etc/keydb/; \
        sed -i 's/^\(daemonize .*\)$/# \1/' /etc/keydb/keydb.conf; \
        sed -i 's/^\(dir .*\)$/# \1\ndir \/data/' /etc/keydb/keydb.conf; \
        sed -i 's/^\(logfile .*\)$/# \1/' /etc/keydb/keydb.conf; \
        sed -i 's/protected-mode yes/protected-mode no/g' /etc/keydb/keydb.conf; \
        sed -i 's/^\(bind .*\)$/# \1/' /etc/keydb/keydb.conf; \
        ln -s keydb-cli redis-cli; \
        cd /etc/keydb; \
        ln -s keydb.conf redis.conf; \
        rm -rf /tmp/*
# generate entrypoint script
RUN set -eux; \
        echo '#!/bin/sh' > /usr/local/bin/docker-entrypoint.sh; \
        echo 'set -e' >> /usr/local/bin/docker-entrypoint.sh; \
        echo "# first arg is '-f' or '--some-option'" >> /usr/local/bin/docker-entrypoint.sh; \
        echo "# or first arg is `something.conf`" >> /usr/local/bin/docker-entrypoint.sh; \
        echo 'if [ "${1#-}" != "$1" ] || [ "${1%.conf}" != "$1" ]; then' >> /usr/local/bin/docker-entrypoint.sh; \
        echo '        set -- keydb-server "$@"' >> /usr/local/bin/docker-entrypoint.sh; \
        echo 'fi' >> /usr/local/bin/docker-entrypoint.sh; \
        echo "# allow the container to be started with `--user`" >> /usr/local/bin/docker-entrypoint.sh; \
        echo 'if [ "$1" = "keydb-server" -a "$(id -u)" = "0" ]; then' >> /usr/local/bin/docker-entrypoint.sh; \
        echo "        find . \! -user keydb -exec chown keydb '{}' +" >> /usr/local/bin/docker-entrypoint.sh; \
        echo '        exec gosu keydb "$0" "$@"' >> /usr/local/bin/docker-entrypoint.sh; \
        echo 'fi' >> /usr/local/bin/docker-entrypoint.sh; \
        echo 'exec "$@"' >> /usr/local/bin/docker-entrypoint.sh; \
        chmod +x /usr/local/bin/docker-entrypoint.sh
# set remaining image properties
VOLUME /data
WORKDIR /data
ENV KEYDB_PRO_DIRECTORY=/usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
EXPOSE 6379
CMD ["keydb-server","/etc/keydb/keydb.conf"]
