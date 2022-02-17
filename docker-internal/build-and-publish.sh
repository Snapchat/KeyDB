#!/bin/sh -e

set -o pipefail

# Color for displaying error messages
red=`tput setaf 1`
reset=`tput sgr0`

# Default value for the command line flag

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

FLAGS_tag="latest"
FLAGS_provider="both"
FLAGS_jobs=

timestamp() {
    date "+%m/%d %H:%M:%S"
}

print_usage() {
    echo "Usage:"
    echo "  ./{script} [flags]"
    echo ""
    echo "Flags:"
    echo "  -p, --provider: name of the cloud provider, can be 'gcp', 'aws' or 'both' (default)"
    echo "  -t, --tag: tag of the image. Default is 'latest'"
    echo "  -j, --jobs: the number of jobs when making the build. Default is number of cores on this host"
}

push_image () { # 1 - image, 2 - repo
   export IMAGE_WITH_REPO="$2/$1"
   docker tag $1 ${IMAGE_WITH_REPO}
   echo "`timestamp` publishing image ${IMAGE_WITH_REPO}"
   docker push "${IMAGE_WITH_REPO}"
   echo "`timestamp` image ${IMAGE_WITH_REPO} is pushed"
}

# Processing flags
while [ ! $# -eq 0 ]
do
    # The shift below ensures the unprocessed flag is always at $1
    case "$1" in
        --help | -h)
            print_usage
            exit 0
            ;;
        --provider | -p)
            FLAGS_provider="$2"
            if [[ ${FLAGS_provider} != "gcp" ]] && [[ ${FLAGS_provider} != "aws" ]] && [[ ${FLAGS_provider} != "both" ]];
            then
                echo "${red} For the flag -p/--provider, only valid values are [\"gcp\", \"aws\", \"both\"]."
                exit 1
            fi
            shift
            ;;
        --tag | -t)
            FLAGS_tag="$2"
            shift
            ;;
        --jobs | -j)
            FLAGS_jobs="$2"
            shift
            ;;
        *)
            echo "${red}"
            echo "Unrecognized flag: $1."
            echo "Run with '--help' flag to see the supported flags."
            echo "${reset}"
            exit 1
            ;;
    esac
    shift
done

echo "`timestamp` building image for ${FLAGS_tag}"
export IMAGE_SUFFIX="keydb:${FLAGS_tag}"
docker build --squash --build-arg KEYDB_DIR=. --build-arg MAKE_JOBS=${FLAGS_jobs} -t keydb:latest -f ${DIR}/Dockerfile ${DIR}/..

# Build and publish
if [[ ${FLAGS_provider} == "aws" ]] || [[ ${FLAGS_provider} == "both" ]]
then
    export ECR="520173307535.dkr.ecr.us-east-1.amazonaws.com"
    echo "`timestamp` Preparing to push image to AWS, ECR: ${ECR}"
    aws ecr get-login-password --profile caching-infra-images-editor --region us-east-1 | docker login --username AWS --password-stdin ${ECR}
    push_image ${IMAGE_SUFFIX} ${ECR}
fi

if [[ ${FLAGS_provider} == "gcp" ]] || [[ ${FLAGS_provider} == "both" ]]
then
    export GCR="gcr.io/caching-infra"
    echo "`timestamp` Preparing to push to GCP, GCR: ${GCR}"
    push_image ${IMAGE_SUFFIX} ${GCR}
fi
