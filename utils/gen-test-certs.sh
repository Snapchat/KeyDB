#!/bin/bash

# Generate some test certificates which are used by the regression test suite:
#
#   tests/tls/ca.{crt,key}          Self signed CA certificate.
#   tests/tls/keydb.{crt,key}       A certificate with no key usage/policy restrictions.
#   tests/tls/client.{crt,key}      A certificate restricted for SSL client usage.
#   tests/tls/server.{crt,key}      A certificate restricted fro SSL server usage.
#   tests/tls/keydb.dh              DH Params file.

generate_cert() {
    local name=$1
    local cn="$2"
    local opts="$3"

    local keyfile=tests/tls/${name}.key
    local certfile=tests/tls/${name}.crt

    [ -f $keyfile ] || openssl genrsa -out $keyfile 4096
    openssl req \
        -new -sha256 \
        -subj "/O=KeyDB Test/CN=$cn" \
        -config "tests/tls/openssl.cnf" \
        -key $keyfile | \
        openssl x509 \
            -req -sha256 \
            -CA tests/tls/ca.crt \
            -CAkey tests/tls/ca.key \
            -CAserial tests/tls/ca.txt \
            -CAcreateserial \
            -days 365 \
            $opts \
            -out $certfile
}

mkdir -p tests/tls
[ -f tests/tls/ca.key ] || openssl genrsa -out tests/tls/ca.key 4096
openssl req \
    -x509 -new -nodes -sha256 \
    -key tests/tls/ca.key \
    -days 3650 \
    -subj '/O=KeyDB Test/CN=Certificate Authority' \
    -out tests/tls/ca.crt

cat > tests/tls/openssl.cnf <<_END_
[ req ]
default_bits       = 4096
distinguished_name = req_distinguished_name
req_extensions     = req_ext

[req_distinguished_name]

[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1=san1.keydb.dev
DNS.2=san2.keydb.dev
DNS.3=san3.keydb.dev
IP.1=192.168.0.1
IP.2=8.8.8.8
IP.3=2001:0db8:15::8a2e:0370:7334
email.1=someone@keydb.dev
email.2=someone_else@keydb.dev
URI.1=https://keydb.dev
URI.2=https://google.com


[ server_cert ]
keyUsage = digitalSignature, keyEncipherment
nsCertType = server

[ client_cert ]
keyUsage = digitalSignature, keyEncipherment
nsCertType = client
_END_

generate_cert server "server.keydb.dev" "-extfile tests/tls/openssl.cnf -extensions server_cert -extensions req_ext"
generate_cert client "client.keydb.dev" "-extfile tests/tls/openssl.cnf -extensions client_cert -extensions req_ext"
generate_cert client2 "client2.keydb.dev" "-extfile tests/tls/openssl.cnf -extensions client_cert -extensions req_ext"
generate_cert keydb "generic.keydb.dev" "-extfile tests/tls/openssl.cnf -extensions req_ext"

[ -f tests/tls/keydb.dh ] || openssl dhparam -out tests/tls/keydb.dh 2048
