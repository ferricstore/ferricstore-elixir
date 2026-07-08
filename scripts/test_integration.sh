#!/usr/bin/env bash
set -euo pipefail

IMAGE="${FERRICSTORE_TEST_IMAGE:-ghcr.io/ferricstore/ferricstore:0.7.5}"
CONTAINER="${FERRICSTORE_TEST_CONTAINER:-ferricstore-elixir-integration}"
HOST="${FERRICSTORE_TEST_HOST:-127.0.0.1}"
PORT="${FERRICSTORE_TEST_PORT:-6388}"
URL="${FERRICSTORE_TEST_URL:-ferric://${HOST}:${PORT}}"

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}

trap cleanup EXIT

cleanup

docker run -d --name "$CONTAINER" \
  -e FERRICSTORE_PROTECTED_MODE=false \
  -e FERRICSTORE_NATIVE_ENABLED=true \
  -e FERRICSTORE_NATIVE_BIND=0.0.0.0 \
  -e FERRICSTORE_NATIVE_PORT=6388 \
  -e FERRICSTORE_NATIVE_ADVERTISE_HOST="$HOST" \
  -e FERRICSTORE_NATIVE_ADVERTISE_PORT="$PORT" \
  -p "${PORT}:6388" \
  "$IMAGE" >/dev/null

for _ in $(seq 1 60); do
  if FERRICSTORE_TEST_URL="$URL" mix run -e '
    {:ok, client} = FerricStore.SDK.start_link(url: System.fetch_env!("FERRICSTORE_TEST_URL"), endpoint_policy: :any)
    {:ok, "PONG"} = FerricStore.SDK.ping(client)
    FerricStore.SDK.close(client)
  ' >/dev/null 2>&1; then
    FERRICSTORE_TEST_URL="$URL" mix test --only integration "$@"
    exit 0
  fi

  sleep 1
done

docker logs "$CONTAINER" >&2 || true
echo "FerricStore Docker container did not become ready at $URL" >&2
exit 1
