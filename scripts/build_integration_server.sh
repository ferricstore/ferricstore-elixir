#!/usr/bin/env bash
set -euo pipefail

SERVER_REF="${FERRICSTORE_SERVER_REF:-11456cc0e5f099b72aac56ffe6acd8b6f3fd1624}"
IMAGE="${FERRICSTORE_TEST_IMAGE:-ferricstore-sdk-contract:${SERVER_REF:0:12}}"
SERVER_SOURCE="${FERRICSTORE_SERVER_SOURCE:-}"
TEMP_SOURCE=""

cleanup() {
  if [[ -n "$TEMP_SOURCE" ]]; then
    rm -rf "$TEMP_SOURCE"
  fi
}

trap cleanup EXIT

if [[ -z "$SERVER_SOURCE" ]]; then
  TEMP_SOURCE="$(mktemp -d "${TMPDIR:-/tmp}/ferricstore-sdk-server.XXXXXX")"
  SERVER_SOURCE="$TEMP_SOURCE"

  git -C "$SERVER_SOURCE" init --quiet
  git -C "$SERVER_SOURCE" remote add origin https://github.com/ferricstore/ferricstore.git
  git -C "$SERVER_SOURCE" fetch --quiet --depth 1 origin "$SERVER_REF"
  git -C "$SERVER_SOURCE" checkout --quiet --detach FETCH_HEAD
fi

ACTUAL_REF="$(git -C "$SERVER_SOURCE" rev-parse HEAD)"
if [[ "$ACTUAL_REF" != "$SERVER_REF" ]]; then
  echo "FerricStore source is $ACTUAL_REF; expected $SERVER_REF" >&2
  exit 1
fi

if [[ -n "$(git -C "$SERVER_SOURCE" status --porcelain)" ]]; then
  echo "FerricStore source must be clean before building the pinned image" >&2
  exit 1
fi

docker build --tag "$IMAGE" "$SERVER_SOURCE"
