#!/bin/sh
set -eu

ACTION="configure"
DONKEY_URL="${DONKEY_URL:-}"
DONKEY_USERNAME="${DONKEY_USERNAME:-}"
DRY_RUN=0
BACKUP_PATH=""

usage() {
  cat <<'EOF'
Donkey Docker helper

Usage:
  helper.sh configure --url https://donkey.example.com [--username USER] [--dry-run]
  helper.sh temporary --url https://donkey.example.com
  helper.sh check
  helper.sh restore --backup /path/to/backup.json

configure backs up and merges Docker settings. It never replaces unrelated keys.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    configure|temporary|check|restore) ACTION="$1" ;;
    --url) shift; DONKEY_URL="${1:-}" ;;
    --username) shift; DONKEY_USERNAME="${1:-}" ;;
    --backup) shift; BACKUP_PATH="${1:-}" ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

require() {
  command -v "$1" >/dev/null 2>&1 || { printf 'Required command not found: %s\n' "$1" >&2; exit 1; }
}

normalize_url() {
  case "$DONKEY_URL" in
    https://*|http://*) ;;
    '') printf 'Missing --url\n' >&2; exit 2 ;;
    *) DONKEY_URL="https://${DONKEY_URL}" ;;
  esac
  DONKEY_URL="${DONKEY_URL%/}"
}

registry_host() {
  printf '%s' "$DONKEY_URL" | sed -E 's#^https?://##; s#/.*$##'
}

merge_json() {
  target="$1"
  key="$2"
  require python3
  temp_file="$(mktemp "${TMPDIR:-/tmp}/donkey-config.XXXXXX")"
  trap 'rm -f "$temp_file"' EXIT HUP INT TERM
  python3 - "$target" "$key" "$DONKEY_URL" >"$temp_file" <<'PY'
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
key = sys.argv[2]
mirror = sys.argv[3]
data = {}
if path.exists() and path.stat().st_size:
    data = json.loads(path.read_text(encoding="utf-8"))
mirrors = data.get(key) or []
if mirror not in mirrors:
    mirrors.insert(0, mirror)
data[key] = mirrors
print(json.dumps(data, ensure_ascii=False, indent=2))
PY
  if [ "$DRY_RUN" -eq 1 ]; then
    printf 'Would write %s:\n' "$target"
    cat "$temp_file"
    return
  fi
  target_dir=$(dirname "$target")
  mkdir -p "$target_dir"
  if [ -f "$target" ]; then
    backup="${target}.donkey.$(date +%Y%m%d%H%M%S).bak"
    cp "$target" "$backup"
    printf 'Backup: %s\n' "$backup"
  fi
  cp "$temp_file" "$target"
  printf 'Updated: %s\n' "$target"
}

restart_docker() {
  if [ "$DRY_RUN" -eq 1 ]; then return; fi
  case "$(uname -s)" in
    Linux)
      if command -v systemctl >/dev/null 2>&1; then systemctl restart docker; else printf 'Restart Docker manually.\n'; fi
      ;;
    Darwin)
      if docker desktop status >/dev/null 2>&1; then docker desktop restart; else printf 'Restart Docker Desktop to apply the mirror.\n'; fi
      ;;
  esac
}

configure() {
  normalize_url
  case "$(uname -s)" in
    Linux)
      if [ "$(id -u)" -ne 0 ]; then printf 'Linux daemon configuration requires root. Re-run with sudo.\n' >&2; exit 1; fi
      merge_json /etc/docker/daemon.json registry-mirrors
      ;;
    Darwin)
      settings_root="${HOME}/Library/Group Containers/group.com.docker"
      if [ -f "${settings_root}/settings-store.json" ]; then merge_json "${settings_root}/settings-store.json" registryMirrors; else merge_json "${settings_root}/settings.json" registryMirrors; fi
      ;;
    *) printf 'Use helper.ps1 on Windows.\n' >&2; exit 1 ;;
  esac
  restart_docker
  if [ -n "$DONKEY_USERNAME" ] && [ "$DRY_RUN" -eq 0 ]; then
    require docker
    printf 'Password for %s: ' "$DONKEY_USERNAME" >&2
    stty -echo
    IFS= read -r donkey_password
    stty echo
    printf '\n' >&2
    printf '%s' "$donkey_password" | docker login "$(registry_host)" --username "$DONKEY_USERNAME" --password-stdin
    unset donkey_password
  fi
}

temporary() {
  normalize_url
  host=$(registry_host)
  cat <<EOF
Temporary use does not change Docker daemon settings:

  docker login ${host}
  docker pull ${host}/library/alpine:latest

Replace library/alpine:latest with the Docker Hub image path you need.
EOF
}

check() {
  require docker
  docker version
  docker info 2>/dev/null | sed -n '/Registry Mirrors:/,/Live Restore Enabled:/p'
}

restore() {
  [ -n "$BACKUP_PATH" ] && [ -f "$BACKUP_PATH" ] || { printf 'restore requires an existing --backup file\n' >&2; exit 2; }
  case "$(uname -s)" in
    Linux) target=/etc/docker/daemon.json ;;
    Darwin)
      settings_root="${HOME}/Library/Group Containers/group.com.docker"
      if [ -f "${settings_root}/settings-store.json" ]; then target="${settings_root}/settings-store.json"; else target="${settings_root}/settings.json"; fi
      ;;
    *) exit 1 ;;
  esac
  if [ "$DRY_RUN" -eq 1 ]; then printf 'Would restore %s to %s\n' "$BACKUP_PATH" "$target"; exit 0; fi
  cp "$BACKUP_PATH" "$target"
  restart_docker
  printf 'Restored: %s\n' "$target"
}

case "$ACTION" in
  configure) configure ;;
  temporary) temporary ;;
  check) check ;;
  restore) restore ;;
esac
