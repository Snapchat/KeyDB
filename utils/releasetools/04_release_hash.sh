#!/bin/bash
SHA=$(curl -s http://download.redis.io/releases/keydb-${1}.tar.gz | shasum -a 256 | cut -f 1 -d' ')
ENTRY="hash keydb-${1}.tar.gz sha256 $SHA http://download.redis.io/releases/keydb-${1}.tar.gz"
echo $ENTRY >> ~/hack/keydb-hashes/README
vi ~/hack/keydb-hashes/README
echo "Press any key to commit, Ctrl-C to abort)."
read yes
(cd ~/hack/keydb-hashes; git commit -a -m "${1} hash."; git push)
