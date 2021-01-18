#!/usr/bin/env bash

set -a
[ -f .env ] && . .env
set +a

[ -z "$S3_UPLOADS_BUCKET" ] && echo "Env var S3_UPLOADS_BUCKET must be defined" && exit 1

aws s3 rm "s3://$S3_UPLOADS_BUCKET" --recursive
