#!/bin/bash

set -e
cd "$(dirname "$0")"

IMAGE=maxhardt90/cde-cli:latest
docker buildx build --platform linux/amd64,linux/arm64 --tag $IMAGE --push .
