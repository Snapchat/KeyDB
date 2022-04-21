This Dockerfile will clone the KeyDB repo, build, and generate a Docker image you can use

To build, use experimental mode to enable use of build args. Tag the build and specify branch name. The command below will generate your docker image:

```
DOCKER_CLI_EXPERIMENTAL=enabled docker build --build-arg BRANCH=<keydbBranch> -t <yourImageName>
```
