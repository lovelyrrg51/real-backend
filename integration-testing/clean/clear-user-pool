#!/usr/bin/env bash

set -a
[ -f .env ] && . .env
set +a

[ -z "$COGNITO_USER_POOL_ID" ] && echo "Env var COGNITO_USER_POOL_ID must be defined" && exit 1

paginationToken=""
while :; do
  cmd="""aws cognito-idp list-users --user-pool-id "$COGNITO_USER_POOL_ID""""
  [ ! -z "$paginationToken" ] && cmd+=""" --pagination-token "$paginationToken""""
  resp=$($cmd)
  paginationToken=$(echo $resp | jq -r .PaginationToken)
  usernames=$(echo $resp | jq -r '.Users | .[] | .Username')
  echo "$usernames" |
  while read username; do \
    echo "Deleting $username"
    aws cognito-idp admin-delete-user --user-pool-id $COGNITO_USER_POOL_ID --username $username;
  done
  [ -z "$paginationToken" ] || [ "$paginationToken" == "null" ] && break
  echo "Iterating with paginationToken: $paginationToken"
done
