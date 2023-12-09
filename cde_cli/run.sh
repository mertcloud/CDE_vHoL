#!/bin/bash

set -e
cd "$(dirname "$0")"

IMAGE=maxhardt90/cde-cli:latest
docker run -v ./config:/home/cdeuser/config --env-file ./config/env.cfg -it $IMAGE bash
