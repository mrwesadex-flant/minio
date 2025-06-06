#!/bin/bash
set -euo pipefail

# Configuration
MC=~/development/minio/mc
ALIAS="local"
ENDPOINT="http://storage-san-test-0:9000"
ACCESS_KEY="minioadmin"
SECRET_KEY="minio-strong-secret"
BUCKET="test-bucket"

function usage() {
  echo "Usage: $0 <prefix: small|large> <start> <end> <size: e.g., 1K, 10M, 10G>"
  echo "Example:"
  echo "  $0 small 0 9999 1K     # generate and upload 10,000 small files of 1KB"
  echo "  $0 large 0 99 10G      # stream-upload 100 large files of 10GB"
  exit 1
}

# Validate arguments
if [[ $# -ne 4 ]]; then
  usage
fi

PREFIX="$1"
START="$2"
END="$3"
SIZE_INPUT="$4"

# Validate prefix
if [[ "$PREFIX" != "small" && "$PREFIX" != "large" ]]; then
  echo "Error: Prefix must be 'small' or 'large'"
  usage
fi

# Validate start and end are integers
if ! [[ "$START" =~ ^[0-9]+$ && "$END" =~ ^[0-9]+$ && "$END" -ge "$START" ]]; then
  echo "Error: Start and end must be integers, and end >= start"
  usage
fi

# Validate and extract size input
if [[ "$SIZE_INPUT" =~ ^([0-9]+)([KMGTP])$ ]]; then
  SIZE_NUM="${BASH_REMATCH[1]}"
  SIZE_UNIT="${BASH_REMATCH[2]}"
else
  echo "Error: Size must be in form 1K, 10M, 33G, etc."
  usage
fi

# Convert size to bytes
case "$SIZE_UNIT" in
  K) SIZE_BYTES=$((SIZE_NUM * 1024)) ;;
  M) SIZE_BYTES=$((SIZE_NUM * 1024 ** 2)) ;;
  G) SIZE_BYTES=$((SIZE_NUM * 1024 ** 3)) ;;
  T) SIZE_BYTES=$((SIZE_NUM * 1024 ** 4)) ;;
  P) SIZE_BYTES=$((SIZE_NUM * 1024 ** 5)) ;;
  *) echo "Error: Unsupported unit '$SIZE_UNIT'"; exit 1 ;;
esac

# Setup alias
$MC alias set $ALIAS $ENDPOINT $ACCESS_KEY $SECRET_KEY > /dev/null

# Ensure bucket exists
if ! $MC ls $ALIAS/$BUCKET > /dev/null 2>&1; then
  $MC mb $ALIAS/$BUCKET
fi

if [[ "$PREFIX" == "small" ]]; then
  echo "Generating small files locally..."

  mkdir -p "$PREFIX"

  for i in $(seq -w $START $END); do
    FILE="$PREFIX/file_${i}.txt"
y
  done

  # Check if remote prefix already exists
#   if $MC ls "$ALIAS/$BUCKET/$PREFIX/" > /dev/null 2>&1; then
#     echo "Error: '$PREFIX/' folder already exists in S3 bucket. Aborting upload."
#     echo "To retry, delete the folder or choose a different prefix."
#     rm -rf "$TMP_DIR"
#     exit 1
#   fi

  echo "Uploading folder to S3: $PREFIX/"
  $MC mirror "$PREFIX" "$ALIAS/$BUCKET/$PREFIX"
  echo "Upload complete."

  rm -rf "$TMP_DIR"

else
  echo "Streaming large files directly to S3..."

  for i in $(seq -w $START $END); do
    FILE_PATH="$ALIAS/$BUCKET/$PREFIX/largefile_${i}.bin"
    echo "Uploading: $FILE_PATH"
    head -c "$SIZE_BYTES" < /dev/zero | $MC pipe "$FILE_PATH"
  done

  echo "Large file uploads complete."
fi
