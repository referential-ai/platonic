#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'docs image smoke: %s\n' "$*" >&2
  exit 1
}

[[ "$#" -eq 1 ]] || die 'usage: smoke-container.sh IMAGE'
readonly image="$1"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly script_dir
site_root="$(cd -- "$script_dir/.." && pwd -P)"
readonly site_root

for command in awk curl docker grep mktemp node rm sed; do
  command -v "$command" >/dev/null 2>&1 || die "required command is unavailable: $command"
done

from_count="$(grep -Ec '^[[:space:]]*FROM[[:space:]]+' "$site_root/Dockerfile")"
pinned_count="$(grep -Ec '^[[:space:]]*FROM[[:space:]]+[^[:space:]@]+@sha256:[0-9a-f]{64}([[:space:]]|$)' "$site_root/Dockerfile")"
[[ "$from_count" -eq 2 && "$pinned_count" -eq "$from_count" ]] \
  || die 'Dockerfile must contain exactly two digest-pinned bases'
grep -E '^[[:space:]]*FROM[[:space:]]+' "$site_root/Dockerfile" | sed 's/^/base: /'

umask 077
proof_root="$(mktemp -d "${TMPDIR:-/tmp}/p559-smoke.XXXXXX")"
chmod 0700 "$proof_root"
readonly proof_root
container_name="platonic-docs-smoke-$$"
readonly container_name
container_started=0

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  set +e
  if [[ "$container_started" -eq 1 ]]; then
    if [[ "$status" -ne 0 ]]; then
      docker logs "$container_name" >&2
    fi
    docker rm --force "$container_name" >/dev/null 2>&1 || status=1
  fi
  rm -rf -- "$proof_root" || status=1
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

docker build --pull --file "$site_root/Dockerfile" --tag "$image" "$site_root"

configured_user="$(docker image inspect "$image" --format '{{.Config.User}}')"
[[ -n "$configured_user" && "$configured_user" != root && "$configured_user" != 0 ]] \
  || die "image user is privileged: ${configured_user:-unset}"
exposed_ports="$(docker image inspect "$image" --format '{{json .Config.ExposedPorts}}')"
[[ "$exposed_ports" == '{"8080/tcp":{}}' ]] \
  || die "image must expose only 8080/tcp: $exposed_ports"

docker run --detach \
  --name "$container_name" \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --publish 127.0.0.1::8080 \
  "$image" >/dev/null
container_started=1

mapping="$(docker port "$container_name" 8080/tcp)"
[[ "$mapping" =~ ^127\.0\.0\.1:([0-9]+)$ ]] || die "unexpected port mapping: $mapping"
readonly base_url="http://127.0.0.1:${BASH_REMATCH[1]}"

for _ in {1..30}; do
  if curl --fail --silent --show-error "$base_url/" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl --fail --silent --show-error "$base_url/" >/dev/null \
  || die 'container did not become ready within 30 seconds'

runtime_uid="$(docker exec "$container_name" id -u)"
[[ "$runtime_uid" =~ ^[0-9]+$ && "$runtime_uid" -ne 0 ]] \
  || die "container is not running as a non-root user: $runtime_uid"
docker exec "$container_name" sh -eu -c '
  ! command -v node >/dev/null 2>&1
  test ! -e /build
  test -f /usr/share/nginx/html/index.html
  test -f /usr/share/nginx/html/404.html
  test -f /usr/share/nginx/html/pagefind/pagefind.js
'

headers="$proof_root/headers"
body="$proof_root/body"

header_value() {
  awk -v expected="$1" '
    {
      sub(/\r$/, "")
      separator = index($0, ":")
      name = substr($0, 1, separator - 1)
      if (separator > 0 && tolower(name) == tolower(expected)) {
        value = substr($0, separator + 1)
        sub(/^[[:space:]]+/, "", value)
      }
    }
    END { print value }
  ' "$headers"
}

request() {
  local path="$1"
  local expected_status="$2"
  local expected_type="$3"
  local status content_type
  status="$(curl --silent --show-error --dump-header "$headers" --output "$body" \
    --write-out '%{http_code}' "$base_url$path")"
  [[ "$status" == "$expected_status" ]] \
    || die "$path returned HTTP $status, expected $expected_status"
  content_type="$(header_value Content-Type)"
  [[ "$content_type" == "$expected_type"* ]] \
    || die "$path returned Content-Type $content_type, expected $expected_type"
}

require_header() {
  local name="$1"
  local expected="$2"
  local actual
  actual="$(header_value "$name")"
  [[ "$actual" == "$expected" ]] \
    || die "header $name is '$actual', expected '$expected'"
}

request / 200 text/html
grep -F 'Platonic documentation' "$body" >/dev/null || die 'root page content is missing'
grep -F '<link rel="canonical" href="https://docs.referential.ai/"' "$body" >/dev/null \
  || die 'root canonical URL is not the production documentation host'
require_header Cache-Control 'public, max-age=0, must-revalidate'
require_header Permissions-Policy 'camera=(), geolocation=(), microphone=()'
require_header Referrer-Policy strict-origin-when-cross-origin
require_header X-Content-Type-Options nosniff
require_header X-Frame-Options DENY

request /user/first-run/ 200 text/html
grep -F 'First run' "$body" >/dev/null || die 'representative page content is missing'

request /pagefind/pagefind.js 200 application/javascript
[[ -s "$body" ]] || die 'Pagefind search asset is empty'
DOCS_BROWSER_ARTIFACTS="$proof_root/browser" DOCS_BROWSER_ORIGIN="$base_url" \
  node "$site_root/node_modules/@playwright/test/cli.js" test \
  --config "$site_root/scripts/browser-playwright.config.mjs" \
  --grep 'root deployment.*search selection and theme persistence'
require_header Content-Security-Policy "default-src 'none'; base-uri 'self'; connect-src 'self'; font-src 'self'; form-action 'none'; frame-ancestors 'none'; img-src 'self' data:; script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval'; style-src 'self' 'unsafe-inline'; worker-src 'self' blob:"

request /sitemap-index.xml 200 application/xml
grep -F '<sitemapindex' "$body" >/dev/null || die 'sitemap index content is missing'

request /robots.txt 200 text/plain
grep -F 'Sitemap: https://docs.referential.ai/sitemap-index.xml' "$body" >/dev/null \
  || die 'robots sitemap declaration is missing'

asset="$(docker exec "$container_name" sh -c \
  "find /usr/share/nginx/html/_astro -type f -name '*.css' | sort | head -n 1")"
asset="${asset#/usr/share/nginx/html}"
[[ "$asset" == /_astro/*.css ]] || die "could not select a generated stylesheet: $asset"
request "$asset" 200 text/css
require_header Cache-Control 'public, max-age=31536000, immutable'

fragment="$(docker exec "$container_name" sh -c \
  "find /usr/share/nginx/html/pagefind/fragment -type f -name '*.pf_fragment' | sort | head -n 1")"
fragment="${fragment#/usr/share/nginx/html}"
[[ "$fragment" == /pagefind/fragment/*.pf_fragment ]] \
  || die "could not select a Pagefind fragment: $fragment"
request "$fragment" 200 application/octet-stream
require_header Cache-Control 'public, max-age=31536000, immutable'

status="$(curl --silent --show-error --header 'Accept-Encoding: gzip' \
  --dump-header "$headers" --output "$body" --write-out '%{http_code}' \
  "$base_url/pagefind/pagefind.js")"
[[ "$status" == 200 ]] || die "compressed search asset returned HTTP $status"
require_header Content-Encoding gzip
require_header Vary Accept-Encoding

request /__platonic_docs_missing__ 404 text/html
grep -F 'Page not found' "$body" >/dev/null || die 'custom 404 content is missing'
require_header Cache-Control 'public, max-age=0, must-revalidate'
require_header X-Content-Type-Options nosniff

printf 'docs image smoke passed: image=%s user=%s uid=%s port=8080 pages/search/sitemap/robots/assets/gzip/headers/404=ok\n' \
  "$image" "$configured_user" "$runtime_uid"
