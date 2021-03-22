FROM alpine

WORKDIR /repo

RUN apk add --no-cache coreutils gcc linux-headers make musl-dev util-linux-dev openssl-dev curl-dev g++ bash git perl tcl

COPY . .

RUN make distclean && make -j

CMD ["./runtest"]
