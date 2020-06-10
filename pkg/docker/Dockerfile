FROM ubuntu:18.04
# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN groupadd -r keydb && useradd -r -g keydb keydb

# grab gosu for easy step-down from root
# https://github.com/tianon/gosu/releases
ENV GOSU_VERSION 1.11
RUN set -eux; \
# save list of currently installed packages for later so we can clean up
        savedAptMark="$(apt-mark showmanual)"; \
        apt-get update; \
        apt-get install -y --no-install-recommends \
                ca-certificates \
                dirmngr \
                gnupg \
                wget \
        ; \
        \
        dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
        wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch"; \
        wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch.asc"; \
        \
# verify the signature
        export GNUPGHOME="$(mktemp -d)"; \
        gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4; \
        gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
        gpgconf --kill all; \
        rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
        \
# clean up fetch dependencies
        apt-mark auto '.*' > /dev/null; \
        [ -z "$savedAptMark" ] || apt-mark manual $savedAptMark > /dev/null; \
        apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
        \
        chmod +x /usr/local/bin/gosu; \
# verify that the binary works
        gosu --version; \
        gosu nobody true

# Load pre-generated equivalent binaries to image (reduces deployment build times)
RUN \
  mkdir -p /etc/keydb
ADD ./app/keydb-* /usr/local/bin/
ADD ./app/docker-entrypoint.sh /usr/local/bin/
ADD ./app/*.conf /etc/keydb/
# Set up config and binaries
RUN \
  cd /usr/local/bin && \
  sed -i 's/^\(bind .*\)$/# \1/' /etc/keydb/keydb.conf && \
  sed -i 's/^\(daemonize .*\)$/# \1/' /etc/keydb/keydb.conf && \
  sed -i 's/^\(dir .*\)$/# \1\ndir \/data/' /etc/keydb/keydb.conf && \
  sed -i 's/^\(logfile .*\)$/# \1/' /etc/keydb/keydb.conf && \
  sed -i 's/protected-mode yes/protected-mode no/g' /etc/keydb/keydb.conf && \
  ln -s keydb-cli redis-cli && \
  cd /etc/keydb && \
  ln -s keydb.conf redis.conf

# Ensure deps installed for binaries
RUN set -eux; \
        \
        savedAptMark="$(apt-mark showmanual)"; \
        apt-get install -y --no-install-recommends \
                libcurl4 \
                libc6 \
                libssl1.1 \
                libuuid1 \
                libstdc++6 \
                libgcc1 \
                zlib1g \
                libbz2-1.0 \
                liblz4-1 \
                libsnappy1v5 \
                libzstd1 \
        ; \
        rm -rf /var/lib/apt/lists/*

# create working directories
RUN \
  mkdir /data && chown keydb:keydb /data && \
  mkdir /flash && chown keydb:keydb /flash

VOLUME /data
WORKDIR /data
ENV KEYDB_PRO_DIRECTORY=/usr/local/bin/

#COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

# Expose container port and start KeyDB by default on running container
EXPOSE 6379
CMD ["keydb-pro-server", "/etc/keydb/keydb.conf"]
