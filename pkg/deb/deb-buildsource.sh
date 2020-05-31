#! /bin/bash

# define parameters used in this deb package build
# enter comments as an argument string quoted. If you do not want comments to be added (ie. you have updated in git repo) type None
if [ $# -eq 0 ]; then
        changelog_comments="This is a new source build generated from the deb_build_source.sh script"
else
        changelog_comments=$1
fi
echo $changelog_comments
build=1 #change if updated build number required. Default is 1 as convention for keydb is to update minor version
version=$(grep KEYDB_REAL_VERSION ../../src/version.h | awk '{ printf $3 }' | tr -d \")
majorv="${version:0:1}"
distributor=$(lsb_release --id --short)
if [ "$distributor" == "Debian" ]; then
        distname=+deb$(lsb_release --release --short | cut -d. -f1)u1
elif [ "$distributor" == "Ubuntu" ]; then
        distname=~$(lsb_release --codename --short)1
fi
codename=$(lsb_release --codename --short)
date=$(date +%a," "%d" "%b" "%Y" "%T)
pkg_name=keydb-$majorv:$version$distname

# create build tree
cd ../../../
tar -czvf keydb_$version.orig.tar.gz --force-local KeyDB
cd KeyDB/pkg/deb/
mkdir -p $pkg_name/tmp
if [ "$codename" == "xenial" ]; then
	cp -r debian_dh9 $pkg_name/tmp/debian
else
	cp -r debian $pkg_name/tmp
fi
cp master_changelog $pkg_name/tmp/debian/changelog
mv ../../../keydb_$version.orig.tar.gz ./$pkg_name
cd $pkg_name/tmp
changelog_str="keydb ($majorv:$version-$build$distname) $codename; urgency=medium\n\n  * $version $changelog_comments \n\n -- Ben Schermel <ben@eqalpha.com>  $date +0000\n\n"
if [ $# -eq 0 ]; then
        sed -i "1s/^/$changelog_str\n/" debian/changelog
elif [ $# -eq 1 ] && [ "$1" != "None" ]; then
        sed -i "1s/^/$changelog_str\n/" debian/changelog
fi
sed -i "s/distribution_placeholder/$distname/g" debian/changelog
sed -i "s/codename_placeholder/$codename/g" debian/changelog

# generate required files and structure for pbuilder including .dsc file
debuild -S -sa
cd ../

# create pbuilder chrooted environment and build the deb package
if [ "$codename" == "xenial" ]; then
        sudo pbuilder create --distribution $codename --othermirror "deb http://archive.ubuntu.com/ubuntu $codename universe multiverse"
else
        sudo pbuilder create --distribution $codename
fi
sudo pbuilder --update
sudo pbuilder --build *.dsc

# move new packages to deb_files_generated and clean up
cp /var/cache/pbuilder/result/*$version*.deb ../deb_files_generated
sudo pbuilder clean
cd ../
rm -rf $pkg_name
