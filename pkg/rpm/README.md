### Generate RPM files for the generated binaries

After running make to produce keydb binaries you can run the following script to create rpm package

Usage: 
```
$ cd KeyDB/pkg/rpm
$ sudo ./generate-rpms.sh
```

This rpm script is currently tested on centos 7 and centos 8 builds

Dependencies:
```
yum install -y scl-utils centos-release-scl rpm-build
```
