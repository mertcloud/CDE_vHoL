#!/bin/bash

set -e
cd "$(dirname "$0")"

IMAGE=maxhardt90/cde-cli:latest
docker run -v ${PWD}/config:/home/cdeuser/.cde -it $IMAGE bash
