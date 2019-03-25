FROM ubuntu:18.04



RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -qqy \
        build-essential nasm autotools-dev autoconf libjemalloc-dev tcl tcl-dev uuid-dev \
    && apt-get clean

CMD make
