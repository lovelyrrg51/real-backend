#!/usr/bin/env bash

set -a
[ -f .env ] && . .env
set +a

[ -z "$COGNITO_IDENTITY_POOL_ID" ] && echo "Env var COGNITO_IDENTITY_POOL_ID must be defined" && exit 1

nextToken=""
while :; do
  cmd="""aws cognito-identity list-identities --identity-pool-id "$COGNITO_IDENTITY_POOL_ID" --max-results 60"""
  [ ! -z "$nextToken" ] && cmd+=""" --next-token "$nextToken""""
  resp=$($cmd)
  ids=$(echo $resp | jq -r '.Identities | map(.IdentityId) | join(" ")')
  nextToken=$(echo $resp | jq -r .NextToken)
  if [ ! -z "$ids" ]; then
    echo "Deleting ids: $ids"
    aws cognito-identity delete-identities --identity-ids-to-delete $ids > /dev/null
  fi
  [ -z "$nextToken" ] || [ "$nextToken" == "null" ] && break
  echo "Iterating with nextToken: $nextToken"
done
