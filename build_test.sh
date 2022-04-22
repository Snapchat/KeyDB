# make the build
git submodule init && git submodule update
make BUILD_TLS=yes -j2 KEYDB_CFLAGS='-Werror' KEYDB_CXXFLAGS='-Werror'

# gen-cert
./utils/gen-test-certs.sh

# test-tls
./runtest --clients 2 --verbose --tls

# cluster-test
./runtest-cluster --tls

# sentinel test
./runtest-sentinel

# module tests
./runtest-moduleapi

# rotation test
./runtest-rotation
