# KeyDB Docker Image (Snap Internal)

This docker image builds KeyDB within the image and cleans up afterwards. A few notes about the image:
* use gosu to avoid running as root https://github.com/tianon/gosu
* packages are installed, tracked and cleaned up to minimize container sizes
* keydb-server binary symbols remain for troubleshooting. All other KeyDB binaries are stripped
* keydb.conf added and linked to redis.conf for legacy compatibility and as default config file
* use entrypoint and cmd for best practices. Remove protected-mode during build incase user specifies binary without .conf, or just wants append parameters

## Building the Docker Image Using Local KeyDB Directory

If you have a local keydb-internal repository you would like to generate the binaries from, use the command below. This will simply copy over all the files within the local keydb-internal repo and then build the image. 

One issue with COPY is that it creates an additional layer. If we remove the source directory in a later layer the size of the original COPY layer remains creating a large image, hence we need to squash the layers into one layer. This feature is available with experimental mode enabled. This can be done by modifying /etc/docker/daemon.json to include `"experimental": true,`. You may also be able to pass at the build line as shown below.

Modify the KEYDB_DIR build argument to your local KeyDB repo and update your image tag in the line below

```
DOCKER_CLI_EXPERIMENTAL=enabled docker build --squash --build-arg KEYDB_DIR=. -t myImageName:imageTag -f ./docker-internal/Dockerfile .
```

Please note that directories are relative to the docker build context. You can use the `-f /path/to/Dockerfile` to specify Dockerfile which will also set the build context, your repo location will be relative to it.

### Pushing

#### AWS
There is a script ./build-and-publish.sh to build and push image. This script will push images to caching-infra AWS account and caching-infra GCP project. Make sure this script is not run using root/sudo otherwise you may not be able to access the correct AWS profile, even if everything else is configured correctly.

If you are pushing to ECR, then you need to add this profile config in your ```~/.aws/config```:

```
[profile caching-infra-images-editor]
role_arn = arn:aws:iam::520173307535:role/_Snap_ContainerEditor
output = json
region = us-east-1
source_profile = default
```
and to get permission for assuming role [_Snap_ContainerEditor in account caching-infra](https://lease.sc-corp.net/v2/request_access/aws_resources/aws_account?resource=520173307535&roles=%5B_Snap_ContainerEditor%5D).

Also, if you are using image different from "520173307535.dkr.ecr.us-east-1.amazonaws.com/keydb", then you need to give access to that image to snap-core-prod aws account. That is account where all mesh services are running. Go to your image in AWS Console and add policy:
```
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "AllowImagePullApp",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::307862320347:root"
      },
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer"
      ]
    }
  ]
}
```

#### GCP

In order to publish to GCP, you will need to get [Storage Admin Role in project caching-infra](https://lease.sc-corp.net/v2/request_access/gcp_resources/gcp_project?resource=caching-infra&roles=%5Broles/storage.admin%5D)

For reading image you will need to add your service account to [caching-infra project](https://lease.sc-corp.net/v2/view_iam?resourceType=PRJ&resource=caching-infra) with  "Container Registry Service Agent" role.

#### Example

```
DOCKER_CLI_EXPERIMENTAL=enabled ./build-and-publish.sh
```

### Troubleshooting
If you see error:
```
#11 354.1 g++: fatal error: Killed signal terminated program cc1plus
```
most likely you are hitting memory constraint. If you are running docker build command from the above, then you can try to reduce number of jobs for "make" by adding "--build-arg MAKE_JOBS=<jobs>" argument to lower value (i.e. 2). If you are running ./build-and-publish.sh you can reduce the number of jobs by passing it in args:
```
DOCKER_CLI_EXPERIMENTAL=enabled ./build-and-publish.sh -j 2
```

## Building the Docker Image Using PAT & Clone

This image clones the keydb-internal repo, hence a GHE PAT token or SSH access is needed. See more on [obtaining GHE PAT](https://wiki.sc-corp.net/display/TOOL/Using+the+GHE+API#UsingtheGHEAPI-Step1:PersonalTokens). It is not secure to pass tokens/credentials as build-args, env variables, or COPYing then deleting, so we use secrets. This option is only available with the [Docker BuildKit](https://docs.docker.com/develop/develop-images/build_enhancements/#new-docker-build-secret-information)so the docker build kit must be enabled via `DOCKER_BUILDKIT=1`, or permanently by appending `"features": { "buildkit": true }` to /etc/docker/daemon.json.

#### To Build:

Run the command below updating your info as follows:
* Pass in the branch name as a build argument
*  Tag your image to whatever you want to use
* Add your PAT info to GHE_PersonalAccessToken.txt

```
cd docker_PAT_build
DOCKER_BUILDKIT=1 docker build --no-cache --build-arg BRANCH=keydbpro --secret id=PAT,src=GHE_PersonalAccessToken.txt . -t myImageName:imageTag
```

For a detailed conventional build view pass `--progress=plain` while building. Note that `--no-cache` must be used to ensure your credentials are pulled in as they are not cached.

#### Troubleshooting

If you are building on macOS or with docker frontend you may need to append the following line to the top of the Dockerfile:

```
# syntax=docker/dockerfile:1.3
```

## Using The Image 

#### Bind port

Bind the container to the node/machine by passing the parameter `-p 6379:6379` when you run the docker container

#### Configuration file

By default KeyDB launches by specifiying its internal keydb.conf file at `/etc/keydb/keydb.conf`. If you would like to use your own config file you can link to the config file with `-v /path-to-config-file/keydb.conf:/etc/keydb/keydb.conf` then run the container whose default command is `keydb-server /etc/keydb/keydb.conf`

Alternatively specify any config file location, by specifying the location following the binary to override default: `docker run ThisDockerImage keydb-server /path/to/my/config-file/`. You can also just append parameters to default image with `docker run ThisDockerImage keydb-server /etc/keydb/keydb.conf --dir /data --server-threads 4`.

#### Persistent storage

If persistence is enabled, data is stored in the VOLUME /data, which can be used with --volumes-from some-volume-container or -v /docker/host/dir:/data (see [docs.docker volumes](https://docs.docker.com/storage/volumes/)).

#### Connect via keydb-cli

you can grab the ip of the container with `docker inspect --format '{{ .NetworkSettings.IPAddress }}' mycontainername` then run the following:

```
docker run -it --rm ThisDockerImage keydb-cli -h <ipaddress-from-above> -p 6379
```

