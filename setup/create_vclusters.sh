#!/bin/bash

set -e
cd "$(dirname "$0")"

N_VCLUSTERS=<number-participants>
CLUSTER_ID="<cde-cluster-id>"
CDP_PROFILE="<cdp-cli-profile>"

for i in $(seq 1 $N_VCLUSTERS)
do
    if [[ "$i" -lt "10" ]]
    then n=00$i
    else n=0$i
    fi

    vcluster="virtual-cluster-$n"
    user="user$n"
    echo "Deploying Virtual Cluster: $vcluster for user: $user"

    cdp de create-vc \
        --name $vcluster \
        --acl-users $user \
        --cpu-requests 20 \
        --memory-requests 80Gi \
        --spark-version SPARK3 \
        --vc-tier ALLP \
        --cluster-id $CLUSTER_ID \
        --profile $CDP_PROFILE \
        || true

done
