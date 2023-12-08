#!/bin/bash

set -e
cd "$(dirname "$0")"

docker run -v ./config:/home/cdeuser/config --env-file ./config/env.cfg -it cdecli bash
