### Generate RPM files for the generated binaries

After making the binaries you can run the following script

Usage: 
```
$ cd KeyDB/pkg/rpm
$ sudo ./generate-rpms.sh <version> <release build #>
```

This rpm script is currently tested on centos 7 and centos 8 builds

Dependencies:
```
yum install -y scl-utils centos-release-scl rpm-build
```
