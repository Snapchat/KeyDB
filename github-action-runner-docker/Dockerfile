FROM ubuntu:18.04

ARG RUNNER_VERSION=2.287.1
ARG TOKEN
ARG NAME=internal-runner

USER root

WORKDIR /actions-runner

RUN apt-get update -qq \
  && apt-get install -y curl \
  && curl -o actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz -L https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz \
  && tar xzf ./actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz \
  && ./bin/installdependencies.sh \
  && rm ./actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz \
  && apt-get purge -y \
  && chown 1000:1000 -R /actions-runner

USER 1000

RUN ./config.sh --url https://github.com/EQ-Alpha/KeyDB --token ${TOKEN} logout --name ${NAME} --unattended

ENTRYPOINT ["./run.sh"]
