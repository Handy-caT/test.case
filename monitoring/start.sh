#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

env_file=.env
if [[ ! -e "$env_file" && -z "${GRAFANA_ADMIN_PASSWORD:-}" ]]; then
    umask 077
    password="$(openssl rand -hex 32)"
    (set -o noclobber; printf 'GRAFANA_ADMIN_PASSWORD=%s\n' "$password" > "$env_file") 2>/dev/null || true
    chmod 600 "$env_file"
fi

if [[ -e "$env_file" ]] \
    && ! grep -Eq '^GRAFANA_ADMIN_PASSWORD=.+$' "$env_file" \
    && [[ -z "${GRAFANA_ADMIN_PASSWORD:-}" ]]; then
    printf 'Set GRAFANA_ADMIN_PASSWORD in .env or the environment before starting Grafana.\n' >&2
    exit 1
fi

if [[ "${1:-}" == native ]]; then
    exec docker compose -f compose.yaml -f compose.native.yaml up -d prometheus grafana
fi

exec docker compose up -d --build
