#! /bin/bash

#### grab build and architecture
build=$(lsb_release -c | awk '{ printf $2 }')
arch=$(dpkg --print-architecture)


# Ensure this script covers build
if [ "$arch" != "arm64" ] && [ "$arch" != "amd64" ]; then
	echo "deb packages are currently only available for arm64 and amd64. You are attempting to build using a: $arch architecture"
	exit 2
fi
if [ "$build" != "bionic" ] && [ "$build" != "xenial" ]; then
	echo "deb packages are currently only available for xenial and bionic builds. You are attempting to build using a: $build distribution"
	exit 2
fi
if [ $# -eq 0 ]; then
	version=0.0.0
fi
if [ $# -gt 0 ]; then
	version=$1
fi
#### create the empty directories github would have removed
mkdir $PWD/deb_source/sentinel/usr/bin
mkdir $PWD/deb_source/tools/usr/bin

####### move updated keydb files to their respective locations for build
cp ../../src/keydb* ./deb_source/tools/usr/bin/
# wget keydb-pro-server -O ./deb_source/tools/usr/bin/
cp ./conf/keydb.conf ./deb_source/server/etc/keydb/
cp ./conf/sentinel.conf ./deb_source/sentinel/etc/keydb/

# Updated changelog will be implemented with build type updated if needed
if [ "$build" == "xenial" ]; then
	sed -i 's/bionic/xenial/g' ./changelog.Debian
fi
if [ "$build" == "bionic" ]; then
        sed -i 's/xenial/bionic/g' ./changelog.Debian
fi
gzip ./changelog.Debian
cp ./changelog.Debian.gz $PWD/deb_source/tools/usr/share/doc/keydb-tools/
cp ./changelog.Debian.gz $PWD/deb_source/server/usr/share/doc/keydb-server/
cp ./changelog.Debian.gz $PWD/deb_source/sentinel/usr/share/doc/keydb-sentinel/
cp ./changelog.Debian.gz $PWD/deb_source/all/usr/share/doc/keydb
gzip -d ./changelog.Debian.gz

# remove any old deb packages
rm $PWD/deb_files_generated/*

# update the systemd service file parameters depending on build type. Some options ar not available for xenial
if [ "$build" == "xenial" ]; then
	# if not already commented out, for xenial comment out the following unavailable systemd options
        sed -e '/ProtectKernelModules/ s/^#*/#/' -i $PWD/deb_source/server/lib/systemd/system/keydb-server.service
        sed -e '/ProtectKernelTunables/ s/^#*/#/' -i $PWD/deb_source/server/lib/systemd/system/keydb-server.service
        sed -e '/ProtectControlGroups/ s/^#*/#/' -i $PWD/deb_source/server/lib/systemd/system/keydb-server.service
        sed -e '/RestrictRealtime/ s/^#*/#/' -i $PWD/deb_source/server/lib/systemd/system/keydb-server.service
        sed -e '/RestrictNamespaces/ s/^#*/#/' -i $PWD/deb_source/server/lib/systemd/system/keydb-server.service
	sed -e '/ProtectKernelModules/ s/^#*/#/' -i $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service
        sed -e '/ProtectKernelTunables/ s/^#*/#/' -i $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service
        sed -e '/ProtectControlGroups/ s/^#*/#/' -i $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service
        sed -e '/RestrictRealtime/ s/^#*/#/' -i $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service
        sed -e '/RestrictNamespaces/ s/^#*/#/' -i $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service
fi
if [ "$build" == "bionic" ]; then
	# if bionic, ensure the optios are available incase commented out from xenial
	sed -i '/ProtectKernelModules/s/^#//g' $PWD/deb_source/server/lib/systemd/system/keydb-server.service
        sed -i '/ProtectKernelTunables/s/^#//g' $PWD/deb_source/server/lib/systemd/system/keydb-server.service
        sed -i '/ProtectControlGroups/s/^#//g' $PWD/deb_source/server/lib/systemd/system/keydb-server.service
        sed -i '/RestrictRealtime/s/^#//g' $PWD/deb_source/server/lib/systemd/system/keydb-server.service
        sed -i '/RestrictNamespaces/s/^#//g' $PWD/deb_source/server/lib/systemd/system/keydb-server.service
        sed -i '/ProtectKernelModules/s/^#//g' $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service
        sed -i '/ProtectKernelTunables/s/^#//g' $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service
        sed -i '/ProtectControlGroups/s/^#//g' $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service
        sed -i '/RestrictRealtime/s/^#//g' $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service
        sed -i '/RestrictNamespaces/s/^#//g' $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service
fi
################################################### create keydb common (all) deb package ################################################################

######## create an updated md5sum file ############
rm $PWD/deb_source/all/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/all/usr/share/doc/keydb/NEWS.Debian.gz | cut -d' ' -f1))'  usr/share/doc/keydb/NEWS.Debian.gz' >> $PWD/deb_source/all/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/all/usr/share/doc/keydb/changelog.Debian.gz | cut -d' ' -f1))'  usr/share/doc/keydb/changelog.Debian.gz' >> $PWD/deb_source/all/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/all/usr/share/doc/keydb/changelog.gz | cut -d' ' -f1))'  usr/share/doc/keydb/changelog.gz' >> $PWD/deb_source/all/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/all/usr/share/doc/keydb/copyright | cut -d' ' -f1))'  usr/share/doc/keydb/copyright' >> $PWD/deb_source/all/DEBIAN/md5sums

######## update control file ###########
# Remove version line and add a new one with updated version
sed -i '2d' $PWD/deb_source/all/DEBIAN/control
sed -i -E "1a\Version: 5:$version-1chl1~"$build"1" $PWD/deb_source/all/DEBIAN/control

sed -i '6d' $PWD/deb_source/all/DEBIAN/control
sed -i -E "5a\Depends: keydb-server (<< 5:$version-1chl1~"$build"1.1~), keydb-server (>= 5:$version-1chl1~"$build"1)" $PWD/deb_source/all/DEBIAN/control

######## create new deb package ###########
dpkg-deb -b ./deb_source/all ./deb_files_generated/keydb_$version-1chl1~"$build"1_all.deb


################################################### create keydb-tools deb package ################################################################

######## create an updated md5sum file ############
rm $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/bin/keydb-benchmark | cut -d' ' -f1))'  usr/bin/keydb-benchmark' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/bin/keydb-check-aof | cut -d' ' -f1))'  usr/bin/keydb-check-aof' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/bin/keydb-check-rdb | cut -d' ' -f1))'  usr/bin/keydb-check-rdb' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/bin/keydb-server | cut -d' ' -f1))'  usr/bin/keydb-server' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/bin/keydb-pro-server | cut -d' ' -f1))'  usr/bin/keydb-pro-server' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/bin/keydb-cli | cut -d' ' -f1))'  usr/bin/keydb-cli' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/bash-completion/completions/keydb-cli | cut -d' ' -f1))'  usr/share/bash-completion/completions/keydb-cli' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/doc/keydb-tools/NEWS.Debian.gz | cut -d' ' -f1))'  usr/share/doc/keydb-tools/NEWS.Debian.gz' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/doc/keydb-tools/changelog.Debian.gz | cut -d' ' -f1))'  usr/share/doc/keydb-tools/changelog.Debian.gz' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/doc/keydb-tools/changelog.gz | cut -d' ' -f1))'  usr/share/doc/keydb-tools/changelog.gz' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/doc/keydb-tools/copyright | cut -d' ' -f1))'  usr/share/doc/keydb-tools/copyright' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/doc/keydb-tools/examples/lru/README | cut -d' ' -f1))'  usr/share/doc/keydb-tools/examples/lru/README' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/doc/keydb-tools/examples/lru/lfu-simulation.c.gz | cut -d' ' -f1))'  usr/share/doc/keydb-tools/examples/lru/lfu-simulation.c.gz' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/doc/keydb-tools/examples/lru/test-lru.rb.gz | cut -d' ' -f1))'  usr/share/doc/keydb-tools/examples/lru/test-lru.rb.gz' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/doc/keydb-tools/examples/redis-trib.rb | cut -d' ' -f1))'  usr/share/doc/keydb-tools/examples/redis-trib.rb' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/man/man1/keydb-benchmark.1.gz | cut -d' ' -f1))'  usr/share/man/man1/keydb-benchmark.1.gz' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/man/man1/keydb-check-aof.1.gz | cut -d' ' -f1))'  usr/share/man/man1/keydb-check-aof.1.gz' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/man/man1/keydb-check-rdb.1.gz | cut -d' ' -f1))'  usr/share/man/man1/keydb-check-rdb.1.gz' >> $PWD/deb_source/tools/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/tools/usr/share/man/man1/keydb-cli.1.gz | cut -d' ' -f1))'  usr/share/man/man1/keydb-cli.1.gz' >> $PWD/deb_source/tools/DEBIAN/md5sums

######## update control file ###########
# Remove version line and add a new one with updated version
sed -i '3d' $PWD/deb_source/tools/DEBIAN/control
sed -i -E "2a\Version: 5:$version-1chl1~"$build"1" $PWD/deb_source/tools/DEBIAN/control
sed -i '4d' $PWD/deb_source/tools/DEBIAN/control
sed -i -E "3a\Architecture: $arch" $PWD/deb_source/tools/DEBIAN/control
if [ "$build" == "bionic" ]; then
	sed -i '7d' $PWD/deb_source/tools/DEBIAN/control
	sed -i -E "6a\Depends: adduser, libc6 (>= 2.17), libcurl4 (>= 7.16.2), libgcc1 (>= 1:3.0), libstdc++6 (>= 4.8.1), libuuid1 (>= 2.16), libssl1.1 (>= 1.1.1), zlib1g (>=1.2.11)" $PWD/deb_source/tools/DEBIAN/control
fi
if [ "$build" == "xenial" ]; then
        sed -i '7d' $PWD/deb_source/tools/DEBIAN/control
	sed -i -E "6a\Depends: adduser, libc6 (>= 2.23), libcurl3 (>= 7.47.0), libgcc1 (>= 6.0.1), libstdc++6 (>= 5.4.0), libuuid1 (>= 2.27.1), libssl1.0.0 (>= 1.0.2), zlib1g (>=1.2.8)" $PWD/deb_source/tools/DEBIAN/control
fi
######## create new deb package ###########
dpkg-deb -b ./deb_source/tools ./deb_files_generated/keydb-tools_$version-1chl1~"$build"1_$arch.deb

################################################### create keydb-server deb package ################################################################

######## create an updated md5sum file ############
rm $PWD/deb_source/server/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/server/lib/systemd/system/keydb-server.service | cut -d' ' -f1))'  lib/systemd/system/keydb-server.service' >> $PWD/deb_source/server/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/server/usr/share/doc/keydb-server/NEWS.Debian.gz | cut -d' ' -f1))'  usr/share/doc/keydb-server/NEWS.Debian.gz' >> $PWD/deb_source/server/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/server/usr/share/doc/keydb-server/README.md.gz | cut -d' ' -f1))'  usr/share/doc/keydb-server/README.md.gz' >> $PWD/deb_source/server/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/server/usr/share/doc/keydb-server/changelog.Debian.gz | cut -d' ' -f1))'  usr/share/doc/keydb-server/changelog.Debian.gz' >> $PWD/deb_source/server/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/server/usr/share/doc/keydb-server/changelog.gz | cut -d' ' -f1))'  usr/share/doc/keydb-server/changelog.gz' >> $PWD/deb_source/server/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/server/usr/share/doc/keydb-server/copyright | cut -d' ' -f1))'  usr/share/doc/keydb-server/copyright' >> $PWD/deb_source/server/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/server/usr/share/man/man1/keydb-server.1.gz | cut -d' ' -f1))'  usr/share/man/man1/keydb-server.1.gz' >> $PWD/deb_source/server/DEBIAN/md5sums

######## update control file ###########
# Remove version line and add a new one with updated version
sed -i '3d' $PWD/deb_source/server/DEBIAN/control
sed -i -E "2a\Version: 5:$version-1chl1~"$build"1" $PWD/deb_source/server/DEBIAN/control
sed -i '7d' $PWD/deb_source/server/DEBIAN/control
sed -i -E "6a\Depends: lsb-base (>= 3.2-14), keydb-tools (= 5:$version-1chl1~"$build"1)" $PWD/deb_source/server/DEBIAN/control
sed -i '4d' $PWD/deb_source/server/DEBIAN/control
sed -i -E "3a\Architecture: $arch" $PWD/deb_source/server/DEBIAN/control
######## create new deb package ###########
dpkg-deb -b ./deb_source/server ./deb_files_generated/keydb-server_$version-1chl1~"$build"1_$arch.deb


################################################### create keydb-sentinel deb package ################################################################

######## create an updated md5sum file ############
rm $PWD/deb_source/sentinel/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/sentinel/lib/systemd/system/keydb-sentinel.service | cut -d' ' -f1))'  lib/systemd/system/keydb-sentinel.service' >> $PWD/deb_source/sentinel/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/sentinel/usr/share/doc/keydb-sentinel/NEWS.Debian.gz | cut -d' ' -f1))'  usr/share/doc/keydb-sentinel/NEWS.Debian.gz' >> $PWD/deb_source/sentinel/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/sentinel/usr/share/doc/keydb-sentinel/changelog.Debian.gz | cut -d' ' -f1))'  usr/share/doc/keydb-sentinel/changelog.Debian.gz' >> $PWD/deb_source/sentinel/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/sentinel/usr/share/doc/keydb-sentinel/changelog.gz | cut -d' ' -f1))'  usr/share/doc/keydb-sentinel/changelog.gz' >> $PWD/deb_source/sentinel/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/sentinel/usr/share/doc/keydb-sentinel/copyright | cut -d' ' -f1))'  usr/share/doc/keydb-sentinel/copyright' >> $PWD/deb_source/sentinel/DEBIAN/md5sums
echo $(echo $(md5sum $PWD/deb_source/sentinel/usr/share/man/man1/keydb-sentinel.1.gz | cut -d' ' -f1))'  usr/share/man/man1/keydb-sentinel.1.gz' >> $PWD/deb_source/sentinel/DEBIAN/md5sums

######## update control file ###########
# Remove version line and add a new one with updated version
sed -i '3d' $PWD/deb_source/sentinel/DEBIAN/control
sed -i -E "2a\Version: 5:$version-1chl1~"$build"1" $PWD/deb_source/sentinel/DEBIAN/control
sed -i '7d' $PWD/deb_source/sentinel/DEBIAN/control
sed -i -E "6a\Depends: lsb-base (>= 3.2-14), keydb-tools (= 5:$version-1chl1~"$build"1)" $PWD/deb_source/sentinel/DEBIAN/control
sed -i '4d' $PWD/deb_source/sentinel/DEBIAN/control
sed -i -E "3a\Architecture: $arch" $PWD/deb_source/sentinel/DEBIAN/control
######## create new deb package ###########
dpkg-deb -b ./deb_source/sentinel ./deb_files_generated/keydb-sentinel_$version-1chl1~"$build"1_$arch.deb
