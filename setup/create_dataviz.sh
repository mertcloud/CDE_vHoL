#!/bin/bash

set -e
cd "$(dirname "$0")"

N_DATAVIZ=30
CLUSTER_ID="env-l6l6hf"
CDP_PROFILE="marketing-workshop-1"
USER_GROUP="cemea-vhol-env-user-group"

for i in $(seq 1 $N_DATAVIZ)
do
    # 9 -> user009, 10 -> user010, ... 
    if [[ "$i" -lt "10" ]]
    then n=00$i
    else n=0$i
    fi

    dataviz="dataviz-$n"
    echo "Deploying DataViz instance: $dataviz"
    cdp dw create-data-visualization \
        --cluster-id $CLUSTER_ID \
        --name $dataviz \
        --config userGroups=$USER_GROUP,adminGroups=$USER_GROUP \
        --template-name small \
        --profile $CDP_PROFILE

done
