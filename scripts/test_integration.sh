#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SERVER_REF="${FERRICSTORE_SERVER_REF:-72452814231f592aff051c22fdbe7114476e2879}"
IMAGE="${FERRICSTORE_TEST_IMAGE:-ferricstore-sdk-contract:${SERVER_REF:0:12}}"
CONTAINER="${FERRICSTORE_TEST_CONTAINER:-ferricstore-elixir-integration-$$}"
HOST="${FERRICSTORE_TEST_HOST:-127.0.0.1}"
PORT="${FERRICSTORE_TEST_PORT:-6388}"
URL="${FERRICSTORE_TEST_URL:-ferric://${HOST}:${PORT}}"

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}

trap cleanup EXIT

cleanup

if [[ -z "${FERRICSTORE_TEST_IMAGE:-}" ]] && ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  FERRICSTORE_SERVER_REF="$SERVER_REF" \
    FERRICSTORE_TEST_IMAGE="$IMAGE" \
    "$SCRIPT_DIR/build_integration_server.sh"
fi

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
  if FERRICSTORE_TEST_URL="$URL" mise exec -- mix run -e '
    {:ok, client} = FerricStore.SDK.start_link(url: System.fetch_env!("FERRICSTORE_TEST_URL"), endpoint_policy: :any)
    {:ok, "PONG"} = FerricStore.SDK.ping(client)
    FerricStore.SDK.close(client)
  ' >/dev/null 2>&1; then
    FERRICSTORE_TEST_URL="$URL" mise exec -- mix test --only integration "$@"
    exit 0
  fi

  sleep 1
done

docker logs "$CONTAINER" >&2 || true
echo "FerricStore Docker container did not become ready at $URL" >&2
exit 1
