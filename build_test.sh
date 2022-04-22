# make the build
make BUILD_TLS=yes -j2 KEYDB_CFLAGS='-Werror' KEYDB_CXXFLAGS='-Werror'

# gen-cert
./utils/gen-test-certs.sh

# test-tls
apt-get -y install tcl tcl-tls
./runtest --clients 2 --verbose --tls

# cluster-test
./runtest-cluster --tls

# sentinel test
./runtest-sentinel

# module tests
./runtest-moduleapi

# rotation test
./runtest-rotation
