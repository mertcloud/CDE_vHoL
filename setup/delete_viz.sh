#!/bin/bash

set -e
cd "$(dirname "$0")"

CLUSTER_ID="env-l6l6hf"
CDP_PROFILE="marketing-workshop-1"

# grab viz apps
cdp dw list-data-visualizations \
    --cluster-id $CLUSTER_ID \
    --profile $CDP_PROFILE \
    | jq -r '.dataVisualizations[].id' >> viz.txt

while read viz; do
    echo "Deleting DataViz instance: $viz"
    cdp dw delete-data-visualization \
        --cluster-id $CLUSTER_ID \
        --data-visualization-id $viz \
        --profile $CDP_PROFILE || true
done < viz.txt

rm viz.txt
