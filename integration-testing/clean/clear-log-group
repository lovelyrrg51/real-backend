#!/usr/bin/env bash

set -a
[ -f .env ] && . .env
set +a

if [ "$#" -ne 1 ]; then
  echo >&2 "Usage: $0 <name of log group to delete>"
  exit 1
fi

logGroupName="$1"

nextToken=""
while :; do
  cmd="""aws logs describe-log-streams --log-group-name $logGroupName --order-by LastEventTime --descending --max-items 50"""
  [ ! -z "$nextToken" ] && cmd+=""" --starting-token "$nextToken""""
  resp=$($cmd)
  [ $? -ne 0 ] && exit 1
  logStreams=$(echo $resp | jq -r '.logStreams | map(.logStreamName) | join(" ")')
  nextToken=$(echo $resp | jq -r .NextToken)
  for logStream in $logStreams; do
    echo "Deleting log stream: $logStream"
    aws logs delete-log-stream --log-group-name $logGroupName --log-stream-name $logStream > /dev/null
  done
  [ -z "$nextToken" ] || [ "$nextToken" == "null" ] && break
  echo "Iterating with nextToken: $nextToken"
done
