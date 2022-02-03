#!/bin/bash
set -eu
set -o pipefail

# Color for displaying error messages
red=`tput setaf 1`
reset=`tput sgr0`

# Default value for the command line flag
DEFAULT_VERSION="2.287.1"
DEFAULT_IMG="gcr.io/caching-infra/keydb-github-action-runner:latest"
DEFAULT_NAME="internal-runner"
FLAGS_token=""
FLAGS_version=$DEFAULT_VERSION
FLAGS_img=$DEFAULT_IMG
FLAGS_name=$DEFAULT_NAME


timestamp() {
    date "+%m/%d %H:%M:%S"
}

print_usage() {
    echo "Usage:"
    echo "  ./{script} [flags]"
    echo ""
    echo "Flags:"
    echo "  -t, --token: Token taken from onboarding script from github. (required)"
    echo "  -i, --image: Full docker image name you want to build and push. Default value is ${DEFAULT_IMG}"
    echo "  -v, --version: Action runner version. Can be taked from onboarding script from github. Default version is ${DEFAULT_VERSION}."
    echo "  -n, --name: Action runner name. Default name is ${DEFAULT_NAME}."
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
        --image | -i)
            FLAGS_img="$2"
            shift
            ;;
        --token | -t)
            FLAGS_token="$2"
            shift
            ;;
        --version | -v)
            FLAGS_version="$2"
            shift
            ;;
        --name | -n)
            FLAGS_name="$2"
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

if [[ ${FLAGS_token} == "" ]]; then
    echo "${red} ERROR:token is missing"
    echo ${reset}
    print_usage
    exit 1
fi

echo "Building image ${FLAGS_img} ..."
docker build --build-arg TOKEN=${FLAGS_token} --build-arg RUNNER_VERSION=${FLAGS_version} --build-arg NAME=${FLAGS_name} -t ${FLAGS_img} .
echo "Pushing image ${FLAGS_img}...."
docker push ${FLAGS_img}
