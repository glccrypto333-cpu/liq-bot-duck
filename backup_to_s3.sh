#!/usr/bin/env bash
set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL is required}"
: "${S3_BUCKET:?S3_BUCKET is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"
: "${AWS_DEFAULT_REGION:?AWS_DEFAULT_REGION is required}"

TS="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
FILE="/tmp/liq_bot_duck_${TS}.dump"
GZ_FILE="${FILE}.gz"

echo "Backup started: ${TS}"
echo "pg_dump version:"
pg_dump --version

echo "Dumping database..."
pg_dump "$DATABASE_URL" \
  --format=custom \
  --no-owner \
  --no-privileges \
  --verbose \
  --file="$FILE"

echo "Compressing..."
gzip -9 "$FILE"

echo "Uploading to S3..."
aws s3 cp "$GZ_FILE" "s3://${S3_BUCKET}/postgres/${GZ_FILE##*/}"

echo "Backup uploaded: s3://${S3_BUCKET}/postgres/${GZ_FILE##*/}"
