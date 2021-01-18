#!/usr/bin/env bash

set -a
[ -f .env ] && . .env
set +a

[ -z "$DYNAMO_TABLE" ] && echo "Env var DYNAMO_TABLE must be defined" && exit 1

rightNow=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
effectiveNow=${1:-$rightNow}
maxItems=100  # somewhat arbitrary

nextToken=""
while :; do

  echo "Pulling up to $maxItems user profiles to update to lastManuallyReindexedAt = $effectiveNow"
  cmd=(aws dynamodb scan)
  cmd+=('--table-name')
  cmd+=($DYNAMO_TABLE)
  cmd+=('--projection-expression')
  cmd+=('partitionKey, sortKey')
  cmd+=('--max-items')
  cmd+=($maxItems)
  cmd+=('--filter-expression')
  cmd+=('sortKey = :sk and ( attribute_not_exists(lastManuallyReindexedAt) or lastManuallyReindexedAt < :now )')
  cmd+=('--expression-attribute-values')
  cmd+=("{\":sk\": {\"S\": \"profile\"}, \":now\": {\"S\": \"$effectiveNow\"}}")
  if [ ! -z "$nextToken" ]; then
    cmd+=('--starting-token')
    cmd+=("$nextToken")
  fi

  resp=$("${cmd[@]}")
  nextToken=$(echo $resp | jq -r .NextToken)
  itemsCnt=$(echo $resp | jq '.Items | length')
  echo "Recieved $itemsCnt user profiles from dynamo."
  echo $resp | jq --compact-output '.Items[]' | while read item; do

    cmd=(aws dynamodb update-item)
    cmd+=('--table-name')
    cmd+=($DYNAMO_TABLE)
    cmd+=('--key')
    cmd+=($item)
    cmd+=('--update-expression')
    cmd+=('SET lastManuallyReindexedAt = :now')
    cmd+=('--expression-attribute-values')
    cmd+=("{\":now\": {\"S\": \"$effectiveNow\"}}")
    "${cmd[@]}" &

  done
  wait

  echo "Updated $itemsCnt user profiles from dynamo."

  [ -z "$nextToken" ] || [ "$nextToken" == "null" ] && break
  echo "Iterating with nextToken: $nextToken"
done
