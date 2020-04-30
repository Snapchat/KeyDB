### Generate RPM files for the generated binaries

KeyDB typically includes pro binaries with open source so does not build the binaries here but instead just packages the binaries

Usage: 
```
$ cd KeyDB/pkg/rpm
$ sudo ./generate-rpms.sh <version> <release build #>
```

This rpm script is currently valid for centos 7 and centos 8 builds only

Dependencies:
```
yum install -y scl-utils centos-release-scl rpm-build
```
