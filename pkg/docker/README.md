In order to create a docker image, generate the keydb binaries, copy them to the app directory, copy keydb.conf and sentinel.conf to the app directory as well, then run the following command:

```
$ sudo docker build . -t <yourimagename>
```
