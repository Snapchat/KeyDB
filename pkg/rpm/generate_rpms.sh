#! /bin/bash

### usage sudo ./generate_rpms
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
version=$(grep KEYDB_REAL_VERSION $DIR/../../src/version.h | awk '{ printf $3 }' | tr -d \")
release=1 # by default this will always be 1 for keydb version structure. If build release version needs to be update you can modify here
arch=$(uname -m)
dist=el$(rpm -q --queryformat '%{VERSION}' centos-release | cut -d. -f1)

if [[ "$arch" != "aarch64" ]] && [[ "$arch" != "x86_64" ]]; then
	echo "This script is only valid and tested for aarch64 and x86_64 architectures. You are trying to use: $arch"
fi

# remove any old rpm packages
rm $DIR/rpm_files_generated/keydb*

# generate empty directories that github would otherwise delete (avoids .gitkeep in directory)
mkdir -p $DIR/keydb_build/keydb_rpm/usr/bin
mkdir -p $DIR/keydb_build/keydb_rpm/usr/lib64/redis/modules
mkdir -p $DIR/keydb_build/keydb_rpm/var/lib/keydb
mkdir -p $DIR/keydb_build/keydb_rpm/var/log/keydb

# move binaries to bin
rm $DIR/keydb_build/keydb_rpm/usr/bin/*
cp $DIR/../../src/keydb-* $DIR/keydb_build/keydb_rpm/usr/bin/

# update spec file with build info
sed -i '2d' $DIR/keydb_build/keydb.spec
sed -i -E "1a\Version     : $version" $DIR/keydb_build/keydb.spec
sed -i '3d' $DIR/keydb_build/keydb.spec
sed -i -E "2a\Release     : $release%{?dist}" $DIR/keydb_build/keydb.spec

# yum install -y scl-utils centos-release-scl rpm-build
mkdir -p /root/rpmbuild/BUILDROOT/keydb-pro-$version-$release.$dist.$arch
cp -r $DIR/keydb_build/keydb_rpm/* /root/rpmbuild/BUILDROOT/keydb-pro-$version-$release.$dist.$arch/
rpmbuild -bb $DIR/keydb_build/keydb.spec
mv /root/rpmbuild/RPMS/$arch/* $DIR/rpm_files_generated

exit
