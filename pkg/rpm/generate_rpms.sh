#! /bin/bash

### usage sudo ./generate_rpms [open source version] [build release #]

version=$1
release=$2
arch=$(uname -m)
dist=el$(rpm -q --queryformat '%{VERSION}' centos-release)

if [ "$arch" != "aarch64" ] || [ "$arch" != "x86_64" ]; then
	echo "This script is only valid and tested for aarch64 and x86_64 architectures. You are trying to use: $arch"
fi
if [ "$dist" != "el7" ] || [ "$dist" != "el8" ]; then
        echo "This script is only valid and tested for centos7 and centos8 builds. You are trying to use: $dist"
fi

# remove any old rpm packages
rm $PWD/rpm_files_generated/keydb*

# generate empty directories that github would otherwise delete (avoids .gitkeep in directory)
mkdir $PWD/centos7_rpm_build/keydb_build/keydb_rpm/usr/bin
mkdir $PWD/centos7_rpm_build/keydb_build/keydb_rpm/usr/lib64/redis/modules
mkdir $PWD/centos7_rpm_build/keydb_build/keydb_rpm/var/lib/keydb
mkdir $PWD/centos7_rpm_build/keydb_build/keydb_rpm/var/log/keydb

# move binaries to bin
rm $PWD/keydb_build/keydb_rpm/usr/bin/*
cp $PWD/../../src/keydb-* $PWD/keydb_build/keydb_rpm/usr/bin/

# update spec file with build info
sed -i '2d' $PWD/keydb_build/keydb.spec
sed -i -E "1a\Version     : $version" $PWD/keydb_build/keydb.spec
sed -i '3d' $PWD/keydb_build/keydb.spec
sed -i -E "2a\Release     : $release%{?dist}" $PWD/keydb_build/keydb.spec

# yum install -y scl-utils centos-release-scl rpm-build
mkdir -p /root/rpmbuild/BUILDROOT/keydb-$version5-$release.$dist.$arch
cp -r ./keydb_build/keydb_rpm/* /root/rpmbuild/BUILDROOT/keydb-$version5-$release.$dist.$arch/
rpmbuild -bb /rpm_build/keydb.spec
mv /root/rpmbuild/RPMS/$arch/* .$PWD/rpm_files_generated

exit
