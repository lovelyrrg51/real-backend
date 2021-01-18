#!/usr/bin/env bash

set -a
[ -f .env ] && . .env
set +a

[ -z "$DYNAMO_TABLE" ] && echo "Env var DYNAMO_TABLE must be defined" && exit 1

nextToken=""
while :; do
  cmd="""aws dynamodb scan --attributes-to-get partitionKey sortKey --table-name "$DYNAMO_TABLE" --max-items 25"""
  [ ! -z "$nextToken" ] && cmd+=""" --starting-token "$nextToken""""
  resp=$($cmd)
  nextToken=$(echo $resp | jq -r .NextToken)
  batchDeleteItems=$(echo $resp | jq '.Items | {DeleteRequest: {Key: .[]}}' | jq --compact-output -s "{\"$DYNAMO_TABLE\": .}")
  batchDeleteCnt=$(echo $resp | jq '.Items | length')
  if [ ! "$batchDeleteCnt" == "0" ]; then
    echo "Deleting a batch of $batchDeleteCnt items"
    aws dynamodb batch-write-item --request-items "$batchDeleteItems" > /dev/null
  fi
  [ -z "$nextToken" ] || [ "$nextToken" == "null" ] && break
  echo "Iterating with nextToken: $nextToken"
done
