## Deb Packaging

The deb packages for open source KeyDB contain the KeyDB-Pro binary as well. As such we are generating deb packages using prebuilt binaries.
Scripts update the appropriate files for the build and `dkpg-deb -b` is used to create the package. Dependencies for Pro are also included. This script modifies the same package rather than having many duplicates to ensure modifications made are pushed accross builds.

If you want to generate your own deb packages following running `make` and generating new binaries you can generate deb packages with the following command:

```
$ cd KeyDB/pkg/deb
$ deb-builder.sh <version-tag>
```

The generated deb packages will be output to the directory "deb_files_generated". Update the changelog prior to generating the deb package if you would like a record.

Github CI will generate packages on each build versioned 0.0.0.0 and will contain open source binaries only. On tagged releases, deb packages with the appropriate version as well as the pro binary will be generated.
 
