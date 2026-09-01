#!/usr/bin/env bash
set -euo pipefail

package="ferricstore_sdk"
expected_version="${PROJECT_VERSION:-}"

if [[ -z "$expected_version" ]]; then
  expected_version="$(
    mix run --no-start --no-compile --no-deps-check \
      -e 'IO.write(Mix.Project.config() |> Keyword.fetch!(:version))'
  )"
fi

if [[ ! "$expected_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]]; then
  echo "Invalid mix.exs version: ${expected_version:-<empty>}" >&2
  exit 1
fi

release_tag="${RELEASE_TAG:-${GITHUB_REF_NAME:-}}"

if [[ -n "${GITHUB_REF_TYPE:-}" && "${GITHUB_REF_TYPE}" != "tag" ]]; then
  echo "Release must run from a tag, not ${GITHUB_REF_TYPE}" >&2
  exit 1
fi

if [[ "$release_tag" != "v${expected_version}" ]]; then
  echo "Release tag ${release_tag:-<empty>} does not match mix.exs version ${expected_version}" >&2
  exit 1
fi

echo "Release tag matches mix.exs version ${expected_version}" >&2
printf 'version=%s\n' "$expected_version"

if [[ "${CHECK_HEX:-false}" != "true" ]]; then
  printf 'already_published=false\n'
  exit 0
fi

hex_api_base="${HEX_API_BASE_URL:-https://hex.pm/api}"
response_file="$(mktemp)"
trap 'rm -f "$response_file"' EXIT

status="$(
  curl --silent --show-error --output "$response_file" --write-out '%{http_code}' \
    "${hex_api_base}/packages/${package}/releases/${expected_version}"
)"

case "$status" in
  200)
    published_checksum="$(jq -r '.checksum' "$response_file")"
    local_checksum="${LOCAL_PACKAGE_CHECKSUM:-}"

    if [[ ! "$published_checksum" =~ ^[0-9a-f]{64}$ ]]; then
      echo "Hex release ${package} ${expected_version} returned an invalid package checksum" >&2
      exit 1
    fi

    if [[ -z "$local_checksum" ]]; then
      package_file="$(mktemp)"
      trap 'rm -f "$response_file" "$package_file"' EXIT
      mix hex.build --output "$package_file" >/dev/null
      local_checksum="$(shasum -a 256 "$package_file" | awk '{print $1}')"
    fi

    if [[ "$local_checksum" != "$published_checksum" ]]; then
      echo "Hex release ${package} ${expected_version} already exists with a different package checksum" >&2
      exit 1
    fi

    echo "Hex release ${package} ${expected_version} already exists with the matching checksum; skipping immutable publish" >&2
    printf 'already_published=true\n'
    ;;
  404)
    echo "Hex release ${package} ${expected_version} is not published yet" >&2
    printf 'already_published=false\n'
    ;;
  *)
    echo "Hex release lookup failed with HTTP ${status}" >&2
    exit 1
    ;;
esac
