# Github action runner.

## Overview
 All details are [here](https://docs.github.com/en/actions/hosting-your-own-runners/about-self-hosted-runners). 
 In brief overview, self-hosted action runner is running on our hosts, connects to the github and waits for the
 jobs to execute. Bellow is instruction on how to create image for the runner and start it in our Kubernetes 
 cluster.

## Creating a new runner.

1. Got [github](https://github.com/EQ-Alpha/KeyDB/settings/actions/runners/new?arch=x64&os=linux) to create a new runner.
2. The link above should display a script for installing runner. It should be aligned with what we have in a
[Dockerfile](https://github.sc-corp.net/Snapchat/keydb-internal/github-action-runner-docker/Dockerfile). 
3. Take the token from that script. Should be in "Configure" section: 
    ``` 
    ./config.sh --url https://github.com/EQ-Alpha/KeyDB --token AUQJRVZIQCLO4ZQOZAOC3L3B7RHIU
    ```
4. Take the version of the runner from the "Download" section from file name. Example is "2.287.1",
5. Build and public the image: 
    ```
   ./build-and-publish.sh --token <token> --version <version>
   ```
    Docker will register runner in Github during the build. If you got an error
```A runner exists with the same name```, it means you've already built the image
for runner with that name. In this case you can either give a new name to the runner
(using ```--name <name>``` parameter to the build script) or delete existing runner in
[github](https://github.com/EQ-Alpha/KeyDB/settings/actions/runners). Deleting the
runner will break existing runners running in our infrastructure. You may want to
delete them.
6. The default image is "[gcr.io/caching-infra/keydb-github-action-runner](https://console.cloud.google.com/gcr/images/caching-infra/global/keydb-github-action-runner?project=caching-infra)"
but you can set any other image full name to the building script (```--image <full-image-name>```). 
7. Deploy new image to cluster [caching-infra--t-us-east4--staging](https://switchboard.sc-corp.net/#/services/caching-infra/cloud-resource/caching-infra--t-us-east4--staging/manage?region=us-east4&provider=GOOGLE&project_id=caching-infra):
   * Configure kubectl for the cluster.
   * Delete existing runner using [deployment manifest](https://github.sc-corp.net/Snapchat/keydb-internal/github-action-runner-docker/deployment.yaml): 
        ```
        kubectl delete -f deployment.yaml
        ```
   * Start a new runner:
        ```
        kubectl apply -f deployment.yaml
        ```
     Deployment manifest uses [gcr.io/caching-infra/keydb-github-action-runner](https://console.cloud.google.com/gcr/images/caching-infra/global/keydb-github-action-runner?project=caching-infra).
     If you specified another image name in build script, update deployment manifest with an appropriate image.
8. Validate on [github](https://github.com/EQ-Alpha/KeyDB/settings/actions/runners) that new runner is online.