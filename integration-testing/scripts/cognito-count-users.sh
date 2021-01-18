#!/usr/bin/env bash

set -a
[ -f .env ] && . .env
set +a

[ -z "$COGNITO_USER_POOL_ID" ] && echo "Env var COGNITO_USER_POOL_ID must be defined" && exit 1

paginationToken=""
confirmedCnt=0
unconfirmedCnt=0
while :; do
  cmd="""aws cognito-idp list-users --user-pool-id "$COGNITO_USER_POOL_ID""""
  [ ! -z "$paginationToken" ] && cmd+=""" --pagination-token "$paginationToken""""
  resp=$($cmd)
  paginationToken=$(echo $resp | jq -r .PaginationToken)
  userstatuses=$(echo $resp | jq -r '.Users | .[] | .UserStatus')
  while read status; do \
    [ "$status" == 'CONFIRMED' ] && ((++confirmedCnt)) && continue
    [ "$status" == 'UNCONFIRMED' ] && ((++unconfirmedCnt)) && continue
    echo "Unexcpected user status: '$status'"
  done <<< "$userstatuses"
  [ -z "$paginationToken" ] || [ "$paginationToken" == "null" ] && break
  echo "Iterating with paginationToken: $paginationToken"
done

echo "Confirmed users: $confirmedCnt"
echo "Unconfirmed users: $unconfirmedCnt"
