FROM ubuntu:18.04

RUN apt-get update \
    && apt-get install -qqy build-essential nasm autotools-dev autoconf libjemalloc-dev \
    && apt-get clean

CMD make