#!/usr/bin/env bash
# Shared deployment lifecycle, process management, GCE bootstrap, log tailing,
# traffic seeding, and report generation helpers for scripts/dev/deploy_test_all.sh.

load_persisted_deploy_env() {
  FLA_ADMIN_DEPLOY_ENV="${FLA_ADMIN_DEPLOY_ENV:-$HOME/.config/fastly-log-analytics/admin/deploy.env}"
  FLA_ADMIN_PERSIST_KEYS=(LOCAL_STANDARD_ADMIN_MTLS GCE_ADMIN_MTLS ELEVATION_ADMIN_MTLS
    ADMIN_GATEWAY_REQUIRED ADMIN_GATEWAY_CALLERS_READY ADMIN_GATEWAY_ENDPOINTS
    ADMIN_GATEWAY_HOST ADMIN_GATEWAY_CERT_DIR ADMIN_GATEWAY_SECRET LOCAL_HOSTS
    ELEVATION_ADMIN_GATEWAY_HOST ELEVATION_ADMIN_GATEWAY_CERT_DIR ELEVATION_ADMIN_GATEWAY_IMAGE
    GCE_ADMIN_BUNDLE_DIR ADMIN_GATEWAY_UID ADMIN_GATEWAY_GID NODE_EXTRA_CA_CERTS)
  if [[ -e "$FLA_ADMIN_DEPLOY_ENV" || -L "$FLA_ADMIN_DEPLOY_ENV" ]]; then
    fla_persisted=$(FLA_KEYS="${FLA_ADMIN_PERSIST_KEYS[*]}" uv run --frozen --quiet python - "$FLA_ADMIN_DEPLOY_ENV" <<'PY' | base64
import os, stat, sys
from dotenv import dotenv_values
path = sys.argv[1]
st = os.lstat(path)
if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
    sys.exit(f"{path}: must be a regular file owned by you with mode 600")
allowed = set(os.environ["FLA_KEYS"].split())
values = dotenv_values(path, interpolate=False)
oneshot = {"GCE_ADMIN_BOOTSTRAP", "GCE_ADMIN_FIREWALL_APPLY"} & values.keys()
if oneshot:
    sys.exit(f"{path}: one-shot keys must not be persisted: {', '.join(sorted(oneshot))}")
unknown = sorted(set(values) - allowed)
if unknown:
    sys.exit(f"{path}: unsupported keys: {', '.join(unknown)}")
for key, value in values.items():
    if value is None or "\0" in value:
        sys.exit(f"{path}: {key} needs a value")
    if key in ("ADMIN_GATEWAY_UID", "ADMIN_GATEWAY_GID") and not value.isdigit():
        sys.exit(f"{path}: {key} must be a numeric id")
    # Node reads exactly one PEM path here; a colon list silently trusts nothing.
    if key == "NODE_EXTRA_CA_CERTS" and not (
        os.path.isabs(value) and ":" not in value and os.path.isfile(value)
    ):
        sys.exit(f"{path}: {key} must be one absolute existing PEM file")
    sys.stdout.write(f"{key}\0{value}\0")
PY
    ) || { echo "Failed to load $FLA_ADMIN_DEPLOY_ENV" >&2; exit 1; }
    while IFS= read -r -d '' fla_key && IFS= read -r -d '' fla_value; do
      if [[ -n "${!fla_key+x}" ]]; then
        if [[ "$fla_key" == *_ADMIN_MTLS && "$fla_value" == "1" && "${!fla_key}" != "1" ]]; then
          echo "WARNING: $fla_key=${!fla_key} from the environment overrides persisted 1 (disables native admin mTLS)" >&2
        fi
        continue
      fi
      printf -v "$fla_key" '%s' "$fla_value"
      export "${fla_key?}"
    done < <(printf '%s' "$fla_persisted" | base64 -d)
    unset fla_persisted fla_key fla_value
    echo "Loaded persisted admin-mTLS settings from $FLA_ADMIN_DEPLOY_ENV"
  fi
}

setup_deploy_gce_ssh() {
  if [[ -n "$GCE_VM_NAME" ]] && gcloud compute ssh "$GCE_VM_NAME" --project="$GCE_PROJECT" --zone="$GCE_ZONE" --command "true" >/dev/null 2>&1; then
    GCE_SSH=(gcloud compute ssh "$GCE_VM_NAME" --project="$GCE_PROJECT" --zone="$GCE_ZONE")
  else
    GCE_SSH_FALLBACK_SCRIPT=$(mktemp "${TMPDIR:-/tmp}/fla_gce_ssh.XXXXXX")
    cat << 'EOF' > "$GCE_SSH_FALLBACK_SCRIPT"
#!/usr/bin/env bash
args=()
cmd=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --command)
      cmd="$2"
      shift 2
      ;;
    *)
      args+=("$1")
      shift
      ;;
  esac
done
key_args=()
if [[ -f "$HOME/.ssh/google_compute_engine" ]]; then
  key_args=(-i "$HOME/.ssh/google_compute_engine")
fi
if [[ "${FLA_BOUNDED_RUN:-0}" == "1" ]]; then
  key_args+=(-o ControlMaster=no -o ControlPath=none)
fi
if [[ -n "$cmd" ]]; then
  exec ssh "${key_args[@]}" -o BatchMode=yes -o StrictHostKeyChecking=no "${GCE_VM_SSH}" "${args[@]}" "$cmd"
else
  exec ssh "${key_args[@]}" -o BatchMode=yes -o StrictHostKeyChecking=no "${GCE_VM_SSH}" "${args[@]}"
fi
EOF
    chmod +x "$GCE_SSH_FALLBACK_SCRIPT"
    GCE_SSH=("$GCE_SSH_FALLBACK_SCRIPT")
  fi
  if [[ "$FLA_BOUNDED_RUN" == "1" && "${GCE_SSH[0]}" == "gcloud" ]]; then
    # A shared, pre-existing SSH mux can retain remote log sessions after the
    # owned local client exits. Give bounded runs their own SSH connections.
    GCE_SSH+=(--ssh-flag=-oControlMaster=no --ssh-flag=-oControlPath=none)
  fi
  GCE_ADMIN_TAG=fla-admin-gateway
  GCE_ADMIN_RULE=fla-admin-gateway-9443
  if [[ "$GCE_ADMIN_BOOTSTRAP" == "1" ]]; then
    [[ "$GCE_ADMIN_MTLS" == "1" ]] || { echo "GCE_ADMIN_BOOTSTRAP requires GCE_ADMIN_MTLS=1" >&2; exit 1; }
    [[ -d "${GCE_ADMIN_BUNDLE_DIR:-}" ]] || { echo "Set GCE_ADMIN_BUNDLE_DIR to the remote-standard gateway bundle" >&2; exit 1; }
    for f in server.crt server.key client-ca.crt; do
      [[ -f "$GCE_ADMIN_BUNDLE_DIR/$f" ]] || { echo "Gateway bundle missing $f" >&2; exit 1; }
    done
  fi
}

# Firewall: TCP 9443 from the verified SSH source /32 to a dedicated tag only.
# An existing rule that differs fails closed; it is never updated or widened.
gce_admin_firewall() {
  local vm_src egress want have
  vm_src=$("${GCE_SSH[@]}" --command 'printf %s "${SSH_CONNECTION%% *}"' 2>/dev/null | tail -1)
  egress=$(curl -fsS --max-time 10 https://api.ipify.org)
  [[ "$vm_src" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || { echo "GCE SSH source is not IPv4" >&2; return 1; }
  [[ "$vm_src" == "$egress" ]] || { echo "GCE SSH source does not match this workstation's egress" >&2; return 1; }
  want="INGRESS|default|tcp:9443|${vm_src}/32|$GCE_ADMIN_TAG"
  if have=$(gcloud compute firewall-rules describe "$GCE_ADMIN_RULE" --project="$GCE_PROJECT" --format=json 2>/dev/null \
      | python3 -c 'import json,sys; r=json.load(sys.stdin); a=r.get("allowed",[]); print("|".join([r["direction"], r["network"].rsplit("/",1)[-1], ",".join(x["IPProtocol"]+":"+",".join(x.get("ports",[])) for x in a), ",".join(r.get("sourceRanges",[])), ",".join(r.get("targetTags",[]))]) + ("|DENY" if r.get("denied") or r.get("disabled") else ""))'); then
    [[ "$have" == "$want" ]] || { echo "Firewall rule $GCE_ADMIN_RULE exists but differs (source changed?); not overwriting" >&2; return 1; }
    echo "Firewall rule $GCE_ADMIN_RULE already matches"
  else
    [[ "$GCE_ADMIN_FIREWALL_APPLY" == "1" ]] || { echo "Firewall rule $GCE_ADMIN_RULE absent; rerun with GCE_ADMIN_FIREWALL_APPLY=1" >&2; return 1; }
    gcloud compute firewall-rules create "$GCE_ADMIN_RULE" --project="$GCE_PROJECT" --network=default \
      --direction=INGRESS --action=ALLOW --rules=tcp:9443 --source-ranges="${vm_src}/32" \
      --target-tags="$GCE_ADMIN_TAG" --priority=1000 --description="FLA admin mTLS gateway from operator /32"
  fi
  gcloud compute instances describe "$GCE_VM_NAME" --project="$GCE_PROJECT" --zone="$GCE_ZONE" \
      --format='value(tags.items)' | tr ';,' '\n\n' | grep -qx "$GCE_ADMIN_TAG" \
    || gcloud compute instances add-tags "$GCE_VM_NAME" --project="$GCE_PROJECT" --zone="$GCE_ZONE" --tags="$GCE_ADMIN_TAG"
}

# One-shot VM bootstrap: gateway-only bundle (no CA key, no client keys),
# VM-generated secret, .env merge, configs/cache ownership, restart.sh patch.
gce_admin_bootstrap() {
  local staging="/tmp/fla-admin-gateway.$$" host="${REMOTE_STD_ORIGIN#https://}"
  host="${host%%:*}"; host="${host%%/*}"
  gce_admin_firewall || return 1
  "${GCE_SSH[@]}" --command "umask 077 && rm -rf $staging && mkdir -m 0700 $staging" || return 1
  gcloud compute scp --project="$GCE_PROJECT" --zone="$GCE_ZONE" \
    "$GCE_ADMIN_BUNDLE_DIR/server.crt" "$GCE_ADMIN_BUNDLE_DIR/server.key" "$GCE_ADMIN_BUNDLE_DIR/client-ca.crt" \
    "${GCE_VM_NAME}:$staging/" || return 1
  "${GCE_SSH[@]}" --command "bash -s -- $host $staging" <<'GCE_ADMIN_VM_BOOTSTRAP'
# Runs ON the VM via: gcloud compute ssh ... --command "bash -s -- HOST STAGING" < vm_bootstrap.sh
# One-shot, idempotent. Never prints secret values or .env contents.
set -euo pipefail
umask 077
HOST="$1"; STAGING="$2"; PORT=9443
APP="$HOME/app"; CERTS=/etc/fastly-log-analytics/admin-gateway
TS=$(date -u +%Y%m%dT%H%M%SZ)
cd "$APP"
for f in docker-compose.admin-mtls.yml docker-compose.admin-mtls.prod.yml deploy/chart/admin-gateway/files/Caddyfile; do
  [[ -f $f ]] || { echo "bootstrap: $f missing; check out the gateway commit first" >&2; exit 1; }
done

# 1. Gateway-only bundle: exactly server.crt, server.key, client-ca.crt.
[[ "$(cd "$STAGING" && ls -A | sort | tr '\n' ' ')" == "client-ca.crt server.crt server.key " ]] \
  || { echo "bootstrap: staging must hold exactly server.crt server.key client-ca.crt" >&2; exit 1; }
openssl pkey -in "$STAGING/server.key" -passin pass: -noout 2>/dev/null \
  || { echo "bootstrap: server.key unreadable or encrypted" >&2; exit 1; }
[[ "$(openssl x509 -in "$STAGING/server.crt" -pubkey -noout)" == "$(openssl pkey -in "$STAGING/server.key" -pubout)" ]] \
  || { echo "bootstrap: server.crt does not match server.key" >&2; exit 1; }
openssl x509 -in "$STAGING/server.crt" -noout -ext subjectAltName | grep -qw "IP Address:$HOST" \
  || { echo "bootstrap: server.crt lacks IP SAN for the gateway host" >&2; exit 1; }
for c in server.crt client-ca.crt; do openssl x509 -in "$STAGING/$c" -checkend 0 -noout >/dev/null; done
openssl x509 -in "$STAGING/client-ca.crt" -noout -ext basicConstraints | grep -q 'CA:TRUE' \
  || { echo "bootstrap: client-ca.crt is not a CA certificate" >&2; exit 1; }
sudo install -d -o 1000 -g 1000 -m 0700 /etc/fastly-log-analytics "$CERTS"
sudo chmod 0755 /etc/fastly-log-analytics
for f in server.crt server.key client-ca.crt; do sudo install -o 1000 -g 1000 -m 0400 "$STAGING/$f" "$CERTS/$f"; done
rm -rf -- "$STAGING"
echo "bootstrap: bundle installed (owner 1000:1000, 0400)"

# 2. .env merge: upsert managed keys only; every other line byte-identical.
MANAGED='^(ADMIN_GATEWAY_HOST|ADMIN_GATEWAY_CERT_DIR|ADMIN_GATEWAY_SECRET|ADMIN_GATEWAY_REQUIRED|ADMIN_GATEWAY_LISTEN_IP|ADMIN_GATEWAY_PORT|LOCAL_HOSTS|COMPOSE_FILE)='
cp -p .env ".env.bak.$TS"; chmod 600 ".env.bak.$TS"
before=$(grep -Ev "$MANAGED" .env | sha256sum)
secret=$(sed -n 's/^ADMIN_GATEWAY_SECRET=//p' .env | tail -1)
[[ ${#secret} -ge 32 && $secret =~ ^[A-Za-z0-9_-]+$ ]] || secret=$(openssl rand -hex 32)
hosts=$(sed -n 's/^LOCAL_HOSTS=//p' .env | tail -1)
[[ ",$hosts," == *",$HOST,"* ]] || hosts="${hosts:+$hosts,}$HOST"
tmp=$(mktemp .env.new.XXXXXX)
{
  grep -Ev "$MANAGED" .env || true
  printf '%s\n' \
    "LOCAL_HOSTS=$hosts" \
    "COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml:docker-compose.admin-mtls.yml:docker-compose.admin-mtls.prod.yml" \
    "ADMIN_GATEWAY_HOST=$HOST" "ADMIN_GATEWAY_CERT_DIR=$CERTS" "ADMIN_GATEWAY_SECRET=$secret" \
    "ADMIN_GATEWAY_REQUIRED=1" "ADMIN_GATEWAY_LISTEN_IP=0.0.0.0" "ADMIN_GATEWAY_PORT=$PORT"
} > "$tmp"
unset secret
chmod 600 "$tmp"
[[ "$(grep -Ev "$MANAGED" "$tmp" | sha256sum)" == "$before" ]] \
  || { rm -f "$tmp"; echo "bootstrap: unmanaged .env lines changed; aborting" >&2; exit 1; }
mv "$tmp" .env
if ! docker compose config --quiet; then
  cp -p ".env.bak.$TS" .env; echo "bootstrap: compose config invalid; .env restored" >&2; exit 1
fi
echo "bootstrap: .env merged (backup .env.bak.$TS, mode 600, $(grep -cE '^[A-Za-z_][A-Za-z0-9_]*=' .env) keys)"

# 3. Ownership: only configs/ and cache/ (Trap #36).
sudo chown -R 1000:1000 configs cache
[[ -z "$(sudo find configs cache ! -uid 1000 -o ! -gid 1000 | head -1)" ]] \
  || { echo "bootstrap: configs/cache ownership not 1000:1000" >&2; exit 1; }
echo "bootstrap: configs/ cache/ owned 1000:1000"

# 4. restart.sh: one recreate via .env COMPOSE_FILE, plus the gateway service.
# Exact-line anchors; each must match once or nothing is written.
R="$HOME/restart.sh"
if ! grep -q 'FLA_ADMIN_GATEWAY_RESTART' "$R"; then
  new=$(mktemp "$HOME/restart.sh.new.XXXXXX")
  awk '
    $0 == "COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)" {
      a++
      print "# FLA_ADMIN_GATEWAY_RESTART: .env COMPOSE_FILE (admin mTLS overlays) wins so"
      print "# backend/frontend are recreated exactly once with ADMIN_GATEWAY_REQUIRED."
      print "if grep -q \"^COMPOSE_FILE=\" .env 2>/dev/null; then"
      print "    COMPOSE=(docker compose)"
      print "else"
      print "    " $0
      print "fi"
      print "GATEWAY=()"
      print "\"${COMPOSE[@]}\" config --services 2>/dev/null | grep -qx admin-gateway && GATEWAY=(admin-gateway)"
      next
    }
    $0 == "if ! \"${COMPOSE[@]}\" build backend frontend; then" {
      b++; print "if ! \"${COMPOSE[@]}\" build backend frontend ${GATEWAY[@]+\"${GATEWAY[@]}\"}; then"; next
    }
    $0 == "\"${COMPOSE[@]}\" up -d --no-deps frontend" {
      c++; print; print "[[ ${#GATEWAY[@]} -eq 0 ]] || \"${COMPOSE[@]}\" up -d --no-deps admin-gateway"; next
    }
    { print }
    END { exit !(a == 1 && b == 1 && c == 1) }
  ' "$R" > "$new" || { rm -f "$new"; echo "bootstrap: restart.sh anchors changed; not patched" >&2; exit 1; }
  bash -n "$new" || { rm -f "$new"; echo "bootstrap: patched restart.sh fails bash -n" >&2; exit 1; }
  cp -p "$R" "$R.bak.$TS"
  chmod --reference="$R" "$new"; mv "$new" "$R"
  echo "bootstrap: restart.sh patched (backup restart.sh.bak.$TS)"
fi

# 5. restart.sh: Caddy must serve the checked-out Caddyfile.
if ! grep -q 'FLA_CADDY_CONFIG_SYNC' "$R"; then
  new=$(mktemp "$HOME/restart.sh.new.XXXXXX")
  awk '
    $0 == "CADDY_ID_BEFORE=\"$(docker inspect -f '"'"'{{.Id}}'"'"' app-caddy-1 2>/dev/null)\"" {
      a++; print
      print "# FLA_CADDY_CONFIG_SYNC: the live bind-mounted Caddyfile must match the checkout."
      print "caddy_config_stale() {"
      print "    [ \"$(docker exec app-caddy-1 cat /etc/caddy/Caddyfile 2>/dev/null | sha256sum)\" != \"$(sha256sum < Caddyfile)\" ]"
      print "}"
      print "[ -n \"$CADDY_ID_BEFORE\" ] && caddy_config_stale && CADDY_CONFIG_CHANGED=1"
      next
    }
    $0 == "    echo \"🔁 Caddyfile changed — reloading in place (zero downtime)\"" {
      b++; print "    echo \"🔁 Caddyfile changed — recreating Caddy so its bind mount sees the new file\""; next
    }
    $0 == "    if ! docker exec app-caddy-1 caddy reload --config /etc/caddy/Caddyfile --adapter caddyfile 2>/dev/null; then" {
      c++; skip = 3; print "    \"${COMPOSE[@]}\" up -d --no-deps --force-recreate caddy"; next
    }
    skip > 0 {
      skip--
      if ((skip == 2 && $0 == "        echo \"   reload failed — falling back to recreate\"") ||
          (skip == 1 && $0 == "        \"${COMPOSE[@]}\" up -d --no-deps caddy") ||
          (skip == 0 && $0 == "    fi")) { d++; next }
      bad = 1; print; next
    }
    $0 == "CADDY_ID_AFTER=\"$(docker inspect -f '"'"'{{.Id}}'"'"' app-caddy-1 2>/dev/null)\"" {
      e++; print
      print "if [ -n \"$CADDY_ID_AFTER\" ] && caddy_config_stale; then"
      print "    echo \"❌ Caddy is serving a stale Caddyfile (bind mount not refreshed)\""
      print "    exit 1"
      print "fi"
      next
    }
    { print }
    END { exit !(a == 1 && b == 1 && c == 1 && d == 3 && e == 1 && !bad) }
  ' "$R" > "$new" || { rm -f "$new"; echo "bootstrap: restart.sh Caddy anchors changed; not patched" >&2; exit 1; }
  bash -n "$new" || { rm -f "$new"; echo "bootstrap: Caddy-patched restart.sh fails bash -n" >&2; exit 1; }
  cp -p "$R" "$R.bak.caddy.$TS"
  chmod --reference="$R" "$new"; mv "$new" "$R"
  echo "bootstrap: restart.sh Caddy config sync patched (backup restart.sh.bak.caddy.$TS)"
fi
echo "bootstrap: done"
GCE_ADMIN_VM_BOOTSTRAP
}

LOCK_DIR="/tmp/fla-deploy-test-all.lock"
LOCK_WAIT_TIMEOUT_S=3600

release_deploy_lock() {
  [[ -d "$LOCK_DIR" ]] || return 0
  local holder
  holder=$(cat "$LOCK_DIR/pid" 2>/dev/null || true)
  [[ "$holder" == "$$" ]] || return 0
  rm -f "$LOCK_DIR/pid"
  rmdir "$LOCK_DIR"
}

kill_pid_tree() {
  local pid="$1"
  local child
  [[ "$pid" =~ ^[0-9]+$ ]] || return 0
  [[ "$pid" -eq "$$" ]] && return 0
  while read -r child; do
    [[ -n "$child" ]] || continue
    kill_pid_tree "$child"
  done < <(pgrep -P "$pid" 2>/dev/null || true)
  kill "$pid" 2>/dev/null || true
}

is_current_process_tree() {
  local candidate="$1"
  local current="$$"
  while [[ "$current" =~ ^[0-9]+$ ]] && [[ "$current" -gt 1 ]]; do
    [[ "$candidate" -eq "$current" ]] && return 0
    current=$(ps -o ppid= -p "$current" 2>/dev/null | tr -d ' ' || true)
  done
  return 1
}

kill_listening_port() {
  local port="$1"
  local pid
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    kill_pid_tree "$pid"
  done < <(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
}

reset_deployment_workflow() {
  echo -e "\n${BOLD}${YELLOW}Resetting active deployment workflow processes...${RESET}"

  local holder_pid=""
  holder_pid=$(cat "$LOCK_DIR/pid" 2>/dev/null || true)
  if [[ "$holder_pid" =~ ^[0-9]+$ ]] && [[ "$holder_pid" -ne "$$" ]]; then
    echo "Stopping prior deploy process tree (PID ${holder_pid})..."
    kill_pid_tree "$holder_pid"
  fi

  local pid command
  while read -r pid command; do
    [[ -n "$pid" ]] || continue
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    is_current_process_tree "$pid" && continue
    if [[ "$command" == *"deploy_test_all.sh"* ]]; then
      echo "Stopping stale deploy process (PID ${pid})..."
      kill_pid_tree "$pid"
    elif [[ "$command" == *"docker-compose"* && "$command" == *"logs -f"* ]] || [[ "$command" == *"docker compose"* && "$command" == *"logs -f"* ]]; then
      echo "Stopping stale docker compose log tailer process (PID ${pid})..."
      kill_pid_tree "$pid"
    fi
  done < <(ps -axo pid=,command= | grep -E 'deploy_test_all\.sh|docker-compose.*logs -f|docker compose.*logs -f' | grep -v grep || true)

  if docker info >/dev/null 2>&1; then
    echo "Stopping local Standard Compose stack..."
    docker compose down --remove-orphans >/dev/null 2>&1 || true
  fi

  if command -v gcloud >/dev/null 2>&1; then
    echo "Stopping stale deployment commands on GCE..."
    "${GCE_SSH[@]}" \
      --command '
        pids=$(ps -eo pid=,args= | awk '"'"'/$HOME\/restart\.sh|deploy_test_all\.sh|docker compose.*(build|up)/ {print $1}'"'"')
        for pid in $pids; do
          [ "$pid" = "$$" ] && continue
          kill "$pid" 2>/dev/null || true
        done
      ' >/dev/null 2>&1 || true
  fi

  if command -v kubectl >/dev/null 2>&1; then
    echo "Stopping stale local Kubernetes rollout/forward commands..."
    while read -r pid; do
      [[ -n "$pid" ]] || continue
      kill_pid_tree "$pid"
    done < <(
      ps -axo pid=,command= |
        awk -v ns="${K8S_NAMESPACE:-}" 'ns != "" && $0 ~ ("kubectl (rollout (status|restart)|set image|set env|port-forward .*" ns ")") {print $1}'
    )
  fi

  rm -rf "$LOCK_DIR"
  echo -e "${GREEN}✅ Deployment workflow reset complete.${RESET}"
}

etime_to_seconds() {
  local t="$1" d=0 a b c h=0 m=0 sec=0
  if [[ "$t" == *-* ]]; then d="${t%%-*}"; t="${t#*-}"; fi
  IFS=: read -r a b c <<< "$t"
  if [[ -n "${c:-}" ]]; then h=$a; m=$b; sec=$c; else m=$a; sec=$b; fi
  echo $(( 10#$d * 86400 + 10#$h * 3600 + 10#$m * 60 + 10#$sec ))
}

is_own_family() {
  local pid="$1" cur
  is_current_process_tree "$pid" && return 0
  cur="$pid"
  while [[ "$cur" =~ ^[0-9]+$ ]] && [[ "$cur" -gt 1 ]]; do
    [[ "$cur" -eq "$$" ]] && return 0
    cur=$(ps -o ppid= -p "$cur" 2>/dev/null | tr -d ' ' || true)
  done
  return 1
}

kill_stale_deploy_processes() {
  local min_age="${1:-0}" self="${BASHPID:-$$}" pid etime cmd age
  local -a victims=()
  while read -r pid etime cmd; do
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    [[ "$pid" -eq "$self" ]] && continue
    is_own_family "$pid" && continue
    age=$(etime_to_seconds "$etime")
    [[ "$age" -ge "$min_age" ]] || continue
    victims+=("$pid")
  done < <(ps -axo pid=,etime=,command= | grep -E 'deploy_test_all\.sh|scripts\.dev\.scale_harness|kubectl logs -f|docker compose logs -f|docker-compose.*logs -f|tail -f.*_(backend|frontend)\.log' | grep -v grep || true)
  [[ "${#victims[@]}" -gt 0 ]] || return 0
  echo -e "${YELLOW}Stopping ${#victims[@]} stale deploy/seeder/tailer process(es) from earlier runs: ${victims[*]}${RESET}"
  for pid in "${victims[@]}"; do kill_pid_tree "$pid"; done
  sleep 3
  for pid in "${victims[@]}"; do kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true; done
}

acquire_lock() {
  local waited=0
  local last_notice=-1000000
  while true; do
    if mkdir "$LOCK_DIR" 2>/dev/null; then
      echo $$ > "$LOCK_DIR/pid"
      trap release_deploy_lock EXIT
      return 0
    fi

    local holder_pid
    holder_pid=$(cat "$LOCK_DIR/pid" 2>/dev/null || echo "")
    if [[ -z "$holder_pid" ]] || ! kill -0 "$holder_pid" 2>/dev/null; then
      echo -e "${YELLOW}Stale lock found (holder PID ${holder_pid:-unknown} is no longer running). Removing it.${RESET}"
      rm -f "$LOCK_DIR/pid"
      rmdir "$LOCK_DIR" || { echo "Lock contains unexpected files; refusing recursive removal." >&2; exit 1; }
      continue
    fi

    if [[ $((waited - last_notice)) -ge 60 ]]; then
      echo -e "${YELLOW}Another deploy_test_all.sh run is already in progress (PID $holder_pid). Waiting for it to finish (${waited}s elapsed)...${RESET}"
      last_notice=$waited
    fi

    if [[ $waited -ge $LOCK_WAIT_TIMEOUT_S ]]; then
      echo -e "${RED}❌ Timed out after $((LOCK_WAIT_TIMEOUT_S / 60)) minute(s) waiting for the prior run (PID $holder_pid) to finish.${RESET}" >&2
      exit 1
    fi

    sleep 10
    waited=$((waited + 10))
  done
}

start_deploy_watchdog() {
  DEPLOY_MAX_RUNTIME_SECONDS="${DEPLOY_MAX_RUNTIME_SECONDS:-5400}"
  DEPLOY_LEFTOVER_MAX_SECONDS="${DEPLOY_LEFTOVER_MAX_SECONDS:-14400}"
  DEPLOY_MAIN_PID=$$
  (
    set +e
    waited=0
    while kill -0 "$DEPLOY_MAIN_PID" 2>/dev/null; do
      if [[ $waited -ge $DEPLOY_MAX_RUNTIME_SECONDS ]]; then
        echo "deploy watchdog: run exceeded ${DEPLOY_MAX_RUNTIME_SECONDS}s; terminating PID ${DEPLOY_MAIN_PID}" >&2
        kill -TERM "$DEPLOY_MAIN_PID" 2>/dev/null
        sleep 10
        kill_pid_tree "$DEPLOY_MAIN_PID"
        kill -9 "$DEPLOY_MAIN_PID" 2>/dev/null
        break
      fi
      sleep 5
      waited=$((waited + 5))
    done
    if [[ "$FLA_BOUNDED_RUN" != "1" ]]; then
      sleep "$DEPLOY_LEFTOVER_MAX_SECONDS"
    fi
  ) >/dev/null 2>&1 &
  DEPLOY_WATCHDOG_PID=$!
}

ensure_colima_docker() {
  if ! docker info >/dev/null 2>&1; then
    if command -v colima >/dev/null 2>&1; then
      echo -e "${YELLOW}⚠️  Docker daemon unreachable — starting Colima (default profile)...${RESET}"
      colima start default
      for _ in $(seq 1 30); do
        docker info >/dev/null 2>&1 && break
        sleep 2
      done
    fi
    if ! docker info >/dev/null 2>&1; then
      echo -e "${RED}❌ Docker daemon still unreachable after attempting to start Colima. Local Standard rollout cannot proceed.${RESET}" >&2
      exit 1
    fi
    echo -e "${GREEN}✅ Docker daemon is now reachable.${RESET}"
  fi
}

start_report_server() {
  if [[ "$FLA_BOUNDED_RUN" == "1" && "$OPEN_BROWSER" == "1" ]]; then
    REPORT_ACCESS_FILE="${REPORT_DIR}/relics/private-report-access.json"
    python3 -m scripts.dev.report_server 0 --private-root "$REPORT_DIR" --access-file "$REPORT_ACCESS_FILE" >"${REPORT_DIR}/logs/report_server.log" 2>&1 &
    REPORT_SERVER_PID=$!
    for _report_attempt in {1..50}; do
      [[ -s "$REPORT_ACCESS_FILE" ]] && break
      if ! kill -0 "$REPORT_SERVER_PID" 2>/dev/null; then
        echo "Private report server failed; see ${REPORT_DIR}/logs/report_server.log" >&2
        exit 1
      fi
      sleep 0.1
    done
    if [[ ! -s "$REPORT_ACCESS_FILE" ]]; then
      echo "Private report server did not become ready." >&2
      exit 1
    fi
    FLA_PRIVATE_REPORT_URL=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["url"])' "$REPORT_ACCESS_FILE")
  elif [[ "$FLA_BOUNDED_RUN" != "1" && ( "$NO_TRAFFIC" != "1" || "$OPEN_BROWSER" == "1" ) ]]; then
    EXISTING_REPORT_SERVER_PID=$(lsof -ti:41705 2>/dev/null || true)
    if [[ -z "$EXISTING_REPORT_SERVER_PID" ]]; then
      python3 -m scripts.dev.report_server 41705 >/dev/null 2>&1 &
      REPORT_SERVER_PID=$!
    fi
  fi
}

set_deploy_status() {
  local environment="$1"
  local status="$2"
  printf '%s\n' "$status" > "${REPORT_DIR}/logs/${environment}_deploy.log"
}

start_container_log_tailers() {
  echo "Waiting for rollout/build..." > "${REPORT_DIR}/logs/local_std_deploy.log"
  echo "Waiting for rollout/build..." > "${REPORT_DIR}/logs/remote_std_deploy.log"
  echo "Waiting for rollout/build..." > "${REPORT_DIR}/logs/remote_hs_deploy.log"

  echo "=== Local Standard Container Logs Started ===" > "${REPORT_DIR}/logs/local_std_backend.log"
  (
    while true; do
      docker compose logs -f --tail=0 backend >> "${REPORT_DIR}/logs/local_std_backend.log" 2>&1 || true
      sleep 1
    done
  ) &
  export LOCAL_STD_BG_PID=$!

  echo "=== Local Standard Frontend Logs Started ===" > "${REPORT_DIR}/logs/local_std_frontend.log"
  (
    while true; do
      docker compose logs -f --tail=0 frontend >> "${REPORT_DIR}/logs/local_std_frontend.log" 2>&1 || true
      sleep 1
    done
  ) &
  export LOCAL_STD_FRONTEND_BG_PID=$!

  echo "=== Remote Standard (GCE) Container Logs Started ===" > "${REPORT_DIR}/logs/remote_std_backend.log"
  (
    sleep 5
    while true; do
      "${GCE_SSH[@]}" --command "cd ~/app && docker compose logs -f --tail=0 backend" >> "${REPORT_DIR}/logs/remote_std_backend.log" 2>&1 || true
      sleep 2
    done
  ) &
  export REMOTE_STD_BG_PID=$!

  echo "=== Remote Standard (GCE) Frontend Logs Started ===" > "${REPORT_DIR}/logs/remote_std_frontend.log"
  (
    sleep 5
    while true; do
      "${GCE_SSH[@]}" --command "cd ~/app && docker compose logs -f --tail=0 frontend" >> "${REPORT_DIR}/logs/remote_std_frontend.log" 2>&1 || true
      sleep 2
    done
  ) &
  export REMOTE_STD_FRONTEND_BG_PID=$!

  echo "=== Remote High-Scale (Elevation) Container Logs Started ===" > "${REPORT_DIR}/logs/remote_hs_backend.log"
  (
    sleep 10
    last_pod=""
    while true; do
      pod="$(kubectl get pods -n "$K8S_NAMESPACE" \
        -l app.kubernetes.io/component=backend \
        -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}' 2>/dev/null | awk '{print $1}')"
      if [[ -n "$pod" && "$pod" != "$last_pod" ]]; then
        last_pod="$pod"
        kubectl logs -f --tail=0 "$pod" -n "$K8S_NAMESPACE" -c backend >> "${REPORT_DIR}/logs/remote_hs_backend.log" 2>&1 || true
      else
        sleep 2
      fi
    done
  ) &
  export REMOTE_HS_BG_PID=$!

  echo "=== Remote High-Scale (Elevation) Frontend Logs Started ===" > "${REPORT_DIR}/logs/remote_hs_frontend.log"
  (
    sleep 10
    last_pod=""
    while true; do
      pod="$(kubectl get pods -n "$K8S_NAMESPACE" \
        -l app.kubernetes.io/component=frontend \
        -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}' 2>/dev/null | awk '{print $1}')"
      if [[ -n "$pod" && "$pod" != "$last_pod" ]]; then
        last_pod="$pod"
        kubectl logs -f --tail=0 "$pod" -n "$K8S_NAMESPACE" >> "${REPORT_DIR}/logs/remote_hs_frontend.log" 2>&1 || true
      else
        sleep 2
      fi
    done
  ) &
  export REMOTE_HS_FRONTEND_BG_PID=$!
}

start_log_monitor() {
  echo -e "\n${BOLD}Starting background Docker container log error/warning monitoring (backend + frontend, all environments)...${RESET}"
  LOG_MONITOR_TEMP=$(mktemp)
  export LOG_MONITOR_TEMP
  (
    docker compose logs -f --tail=0 backend 2>/dev/null | grep --line-buffered -E -i "error|exception|traceback|fatal|critical" | grep --line-buffered -v -E -i "Logging errors to|control-room/error-stream|/rum/errors|overcommit|telemetry-proxy|SSR upstream timeout|ducklake advisory lock timed out.*attempt 1|['\"]errors['\"]:[[:space:]]*0|snapshot-id probe failed|lake not attached|and [0-9]+ errors files|metadata pointer unreadable for .*client_errors|enrich_batch complete|REFUSING .*rollback|already running for this service|legacy adoption FAILED" | sed -u 's/^/[Local Standard\/backend] /' >> "$LOG_MONITOR_TEMP" &
    docker compose logs -f --tail=0 frontend 2>/dev/null | grep --line-buffered -E -i "error|exception|traceback|fatal|critical" | grep --line-buffered -v -E -i "SSR upstream timeout|fetch failed; falling back to client fetch" | sed -u 's/^/[Local Standard\/frontend] /' >> "$LOG_MONITOR_TEMP" &
    docker compose logs -f --tail=0 caddy 2>/dev/null | grep --line-buffered -E -i "error|exception|traceback|fatal|critical" | grep --line-buffered -v -E -i "context canceled|incomplete response|use of closed network connection" | sed -u 's/^/[Local Standard\/caddy] /' >> "$LOG_MONITOR_TEMP" &

    sleep 6
    tail -f -n 0 "${REPORT_DIR}/logs/remote_std_backend.log" 2>/dev/null | grep --line-buffered -E -i "error|exception|traceback|fatal|critical" | grep --line-buffered -v -E -i "control-room/error-stream|/rum/errors|telemetry-proxy|SSR upstream timeout|['\"]errors['\"]:[[:space:]]*0|snapshot-id probe failed|lake not attached|and [0-9]+ errors files|metadata pointer unreadable for .*client_errors|enrich_batch complete|REFUSING .*rollback|already running for this service|legacy adoption FAILED" | sed -u 's/^/[Remote Standard\/backend] /' >> "$LOG_MONITOR_TEMP" &
    tail -f -n 0 "${REPORT_DIR}/logs/remote_std_frontend.log" 2>/dev/null | grep --line-buffered -E -i "error|exception|traceback|fatal|critical" | grep --line-buffered -v -E -i "ECONNREFUSED 127.0.0.1:8000|SSR upstream timeout|fetch failed; falling back to client fetch" | sed -u 's/^/[Remote Standard\/frontend] /' >> "$LOG_MONITOR_TEMP" &
    tail -f -n 0 "${REPORT_DIR}/logs/remote_hs_backend.log" 2>/dev/null | grep --line-buffered -E -i "error|exception|traceback|fatal|critical" | grep --line-buffered -v -E -i "control-room/error-stream|/rum/errors|telemetry-proxy|SSR upstream timeout|snapshot-id probe failed|lake not attached|and [0-9]+ errors files|metadata pointer unreadable for .*client_errors|enrich_batch complete|REFUSING .*rollback|clickhouse.operation.*status_code=404|subscription to .* lost.*reconnecting|already running for this service" | sed -u 's/^/[Remote High-Scale\/backend] /' >> "$LOG_MONITOR_TEMP" &
    tail -f -n 0 "${REPORT_DIR}/logs/remote_hs_frontend.log" 2>/dev/null | grep --line-buffered -E -i "error|exception|traceback|fatal|critical" | grep --line-buffered -v -E -i "ECONNREFUSED 127.0.0.1:8000|socket hang up|SSR upstream timeout|fetch failed; falling back to client fetch" | sed -u 's/^/[Remote High-Scale\/frontend] /' >> "$LOG_MONITOR_TEMP" &

    while true; do sleep 1; done
  ) &
  export MONITOR_BG_PID=$!
}

emit_deploy_report_script() {
  local status="$1"
  cat <<EOF
  <script>
    function ansiToHtml(text) {
      let safe = text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');

      const ansiRegex = /[\u001b\x1b]\[([0-9;]+)m/g;

      return safe.replace(ansiRegex, (match, p1) => {
        const codes = p1.split(';');
        let span = '<span class="';
        let hasStyle = false;

        codes.forEach(code => {
          if (code === '0') {
            span = '</span>';
            hasStyle = false;
          } else if (code === '1') {
            span += 'font-bold ';
            hasStyle = true;
          } else if (code === '32') {
            span += 'text-emerald-400 ';
            hasStyle = true;
          } else if (code === '33') {
            span += 'text-amber-400 ';
            hasStyle = true;
          } else if (code === '31') {
            span += 'text-rose-400 ';
            hasStyle = true;
          } else if (code === '36') {
            span += 'text-cyan-400 ';
            hasStyle = true;
          }
        });

        if (hasStyle) {
          return span + '">';
        } else {
          return '</span>';
        }
      });
    }

    function applyMonitoredErrorsFilter(shouldScroll) {
      const pre = document.getElementById('monitored-errors-log');
      if (!pre) return;

      const text = window.rawMonitoredErrors || '';
      const filterEnv = document.getElementById('filter-env').value;
      const filterContainer = document.getElementById('filter-container').value;
      const filterSeverity = document.getElementById('filter-severity').value;
      const filterSearch = document.getElementById('filter-search').value.trim().toLowerCase();

      if (!text.trim()) {
        const isProgress = "${status}" === "IN_PROGRESS" || "${status}" === "IN PROGRESS";
        if (isProgress) {
          pre.innerHTML = '<span class="text-blue-400 animate-pulse font-sans">● Live background log monitor is active, scanning standard container outputs for exceptions...</span>';
        } else {
          pre.innerHTML = '<span class="text-emerald-400 font-sans">🟢 Live background log monitor completed scans with zero exceptions detected.</span>';
        }
        document.getElementById('filtered-errors-count').textContent = 'Showing 0 / 0 rows';
        return;
      }

      const lines = text.split('\n');
      const totalCount = lines.filter(l => l.trim()).length;
      const filteredLines = [];

      lines.forEach(line => {
        if (!line.trim()) return;

        const match = line.match(/^\[([^/\]]+)(?:\/([^\]]+))?\]/);
        let lineEnv = 'all';
        let lineContainer = 'all';
        if (match) {
          lineEnv = match[1].trim();
          lineContainer = match[2] ? match[2].trim() : 'all';
        }

        if (filterEnv !== 'all' && lineEnv !== filterEnv) return;
        if (filterContainer !== 'all' && lineContainer !== filterContainer) return;

        const isWarning = /warn|warning/i.test(line);
        const isError = /error|exception|traceback|failed|unauthorized/i.test(line);

        if (filterSeverity === 'error' && isWarning && !isError) return;
        if (filterSeverity === 'warning' && !isWarning) return;
        if (filterSearch && !line.toLowerCase().includes(filterSearch)) return;

        filteredLines.push(line);
      });

      const filteredText = filteredLines.join('\n');
      pre.innerHTML = ansiToHtml(filteredText) || '<span class="text-slate-500 italic font-sans">No matching log rows found for the active filters.</span>';
      document.getElementById('filtered-errors-count').textContent = 'Showing ' + filteredLines.length + ' / ' + totalCount + ' rows';

      if (shouldScroll) {
        pre.scrollTop = pre.scrollHeight;
      }
    }

    document.addEventListener('DOMContentLoaded', () => {
      const bindFilter = (id, event) => {
        const el = document.getElementById(id);
        if (el) el.addEventListener(event, () => applyMonitoredErrorsFilter(true));
      };
      bindFilter('filter-env', 'change');
      bindFilter('filter-container', 'change');
      bindFilter('filter-severity', 'change');
      bindFilter('filter-search', 'input');

      const clearBtn = document.getElementById('clear-filters-btn');
      if (clearBtn) {
        clearBtn.addEventListener('click', () => {
          document.getElementById('filter-env').value = 'all';
          document.getElementById('filter-container').value = 'all';
          document.getElementById('filter-severity').value = 'all';
          document.getElementById('filter-search').value = '';
          applyMonitoredErrorsFilter(true);
        });
      }
    });

    async function updateLogPre(id, logPath) {
      try {
        const headRes = await fetchWithRetry(() => fetch(logPath, { method: 'HEAD', cache: 'no-store' }), 2);
        if (headRes.ok) {
          const newLen = headRes.headers.get('Content-Length');
          const pre = document.getElementById(id);
          if (pre && newLen) {
            if (pre.getAttribute('data-last-len') !== newLen) {
              const sel = window.getSelection();
              if (sel && !sel.isCollapsed && pre.contains(sel.anchorNode)) {
                return;
              }

              const res = await fetchWithRetry(() => fetch(logPath, { cache: 'no-store' }), 2);
              if (res.ok) {
                const text = await res.text();
                const wasAtBottom = pre.scrollHeight - pre.clientHeight <= pre.scrollTop + 30;

                if (id === 'monitored-errors-log') {
                  window.rawMonitoredErrors = text;
                  applyMonitoredErrorsFilter(wasAtBottom);
                  pre.setAttribute('data-last-len', newLen);
                } else {
                  pre.innerHTML = ansiToHtml(text);
                  pre.setAttribute('data-last-len', newLen);
                  if (wasAtBottom) {
                    pre.scrollTop = pre.scrollHeight;
                  }
                }
              }
            }
          }
        }
      } catch (e) {}
    }

    const logsMapping = [
      { id: 'log-pre', path: 'logs/deploy.log' },
      { id: 'monitored-errors-log', path: 'logs/monitored_errors.log' },
      { id: 'local-std-log', path: 'logs/local_std_deploy.log' },
      { id: 'remote-std-log', path: 'logs/remote_std_deploy.log' },
      { id: 'remote-hs-log', path: 'logs/remote_hs_deploy.log' },
      { id: 'local-std-backend-log', path: 'logs/local_std_backend.log' },
      { id: 'local-std-frontend-log', path: 'logs/local_std_frontend.log' },
      { id: 'remote-std-backend-log', path: 'logs/remote_std_backend.log' },
      { id: 'remote-std-frontend-log', path: 'logs/remote_std_frontend.log' },
      { id: 'remote-hs-backend-log', path: 'logs/remote_hs_backend.log' },
      { id: 'remote-hs-frontend-log', path: 'logs/remote_hs_frontend.log' }
    ];

    const envsMapping = [
      { id: 'local-std', url: '${LOCAL_STD_ORIGIN}' },
      { id: 'remote-std', url: '${REMOTE_STD_ORIGIN}' },
      { id: 'remote-hs', url: '${REMOTE_HS_ORIGIN}' }
    ];

    function lastCheckedSuffix() {
      return ' \u2022 Last checked: ' + new Date().toLocaleTimeString();
    }

    function setDot(id, suffix, healthy, tipText) {
      const dot = document.getElementById(id + '-' + suffix + '-dot');
      const tip = document.getElementById(id + '-' + suffix + '-tip');
      if (dot) {
        dot.className = healthy
          ? "inline-block w-2 h-2 rounded-full bg-emerald-500 shadow-[0_0_6px_rgba(16,185,129,0.6)]"
          : "inline-block w-2 h-2 rounded-full bg-rose-500 shadow-[0_0_6px_rgba(244,63,94,0.6)]";
      }
      if (tip) {
        tip.textContent = tipText + lastCheckedSuffix();
      }
    }

    async function fetchWithRetry(fn, attempts) {
      let lastErr;
      for (let i = 0; i < attempts; i++) {
        try {
          return await fn();
        } catch (e) {
          lastErr = e;
        }
      }
      throw lastErr;
    }

    async function pingPort(id, url) {
      try {
        if (url.startsWith('https://')) {
          const response = await fetch('relics/bootstrap_status.json?env=' + encodeURIComponent(id),
            { cache: 'no-store', signal: AbortSignal.timeout(8000) });
          const result = await response.json();
          if (!result.ok) throw new Error('Admin TLS diagnostics unavailable');
          setDot(id, 'ports', true, 'Native admin TLS: reachable');
          return true;
        }
        await fetchWithRetry(() => fetch(url + '/api/health', { mode: 'no-cors', cache: 'no-store', signal: AbortSignal.timeout(2500) }), 2);
        setDot(id, 'ports', true, 'Ports: reachable');
        return true;
      } catch (e) {
        setDot(id, 'ports', false, 'Ports: closed / unreachable');
        return false;
      }
    }

    function renderStatsError(id, message) {
      const container = document.getElementById(id + '-stats');
      if (container) {
        container.innerHTML =
          '<div class="mt-1 text-[9px] text-amber-400 font-sans leading-tight border-t border-slate-800/40 pt-1 animate-fadeIn">' +
            '\u26a0 ' + message +
          '</div>';
      }
    }

    async function updateStats(id, url) {
      try {
        const res = await fetchWithRetry(() => fetch('relics/bootstrap_status.json?env=' + encodeURIComponent(id), { cache: 'no-store', signal: AbortSignal.timeout(8000) }), 2);
        const proxied = await res.json();
        if (proxied.ok && proxied.data) {
          const data = proxied.data;
          setDot(id, 'containers', true, 'Containers: application responding normally');
          if (data && data.header_badge) {
            const hb = data.header_badge;
            const reqTotal = hb.request ? hb.request.total_rows : 0;
            const rumTotal = hb.rum ? hb.rum.total_rows : 0;

            const parseTs = (ts) => {
              if (!ts) return null;
              let normalized = String(ts).trim();
              if (normalized.includes(' ') && !normalized.includes('T')) {
                normalized = normalized.replace(' ', 'T');
              }
              if (!/[Zz]|[+-]\d{2}:?\d{2}$/.test(normalized)) {
                normalized += 'Z';
              }
              const d = new Date(normalized);
              return isNaN(d.getTime()) ? null : d;
            };
            const fmtTime = (ts) => {
              const d = parseTs(ts);
              if (!d) return 'Never';
              const diffSec = Math.floor((Date.now() - d.getTime()) / 1000);
              if (diffSec < 5) return 'just now';
              if (diffSec < 60) return diffSec + 's ago';
              const diffMin = Math.floor(diffSec / 60);
              if (diffMin < 60) return diffMin + 'm ago';
              const diffHr = Math.floor(diffMin / 60);
              if (diffHr < 24) return diffHr + 'h ago';
              const diffDay = Math.floor(diffHr / 24);
              return diffDay + 'd ago';
            };
            const reqLatest = hb.request ? fmtTime(hb.request.latest_log_at) : 'Never';
            const rumLatest = hb.rum ? fmtTime(hb.rum.latest_log_at) : 'Never';
            const reqSynced = hb.request ? fmtTime(hb.request.last_sync_at) : 'Never';
            const rumSynced = hb.rum ? fmtTime(hb.rum.last_sync_at) : 'Never';

            const statsHtml =
              '<div class="mt-1 grid grid-cols-2 gap-x-3 gap-y-0.5 text-[9px] text-slate-400 font-sans leading-tight border-t border-slate-800/40 pt-1 animate-fadeIn">' +
                '<div class="font-bold text-slate-300 font-sans">REQUESTS</div>' +
                '<div class="font-bold text-slate-300 font-sans">RUM</div>' +
                '<div>Total: <span class="text-slate-200">' + reqTotal.toLocaleString() + '</span></div>' +
                '<div>Total: <span class="text-slate-200">' + rumTotal.toLocaleString() + '</span></div>' +
                '<div>Latest: <span class="text-slate-200">' + reqLatest + '</span></div>' +
                '<div>Latest: <span class="text-slate-200">' + rumLatest + '</span></div>' +
                '<div>Synced: <span class="text-slate-200">' + reqSynced + '</span></div>' +
                '<div>Synced: <span class="text-slate-200">' + rumSynced + '</span></div>' +
              '</div>';

            const container = document.getElementById(id + '-stats');
            if (container) {
              container.innerHTML = statsHtml;
            }
          } else {
            renderStatsError(id, 'Stats unavailable: application responded without a header_badge payload');
          }
        } else {
          const detail = proxied.error ? proxied.error : ('HTTP ' + (proxied.status || 'error'));
          setDot(id, 'containers', false, 'Containers: port reachable, but application returned ' + detail);
          renderStatsError(id, 'Stats unavailable: ' + detail);
        }
      } catch (e) {
        setDot(id, 'containers', false, 'Containers: no response from application');
        renderStatsError(id, 'Stats unavailable: no response from application (' + (e && e.message ? e.message : e) + ')');
      }
    }

    async function updateCredentialsStatus() {
      try {
        const res = await fetch('relics/credentials_status.json', { cache: 'no-store' });
        if (res.ok) {
          const creds = await res.json();
          envsMapping.forEach(env => {
            const container = document.getElementById(env.id + '-creds');
            if (container && creds[env.id]) {
              const c = creds[env.id];
              const apiDot = c.fastly_api_ok ? '🟢' : '🔴';
              const fosDot = c.fos_bucket_ok ? '🟢' : '🔴';
              const cdnDot = c.cdn_requests_ok ? '🟢' : '🔴';
              const checkedNow = lastCheckedSuffix();
              const apiTip = (c.fastly_api_ok ? 'Fastly API: authenticated' : 'Fastly API: not authenticated') + checkedNow;
              const fosTip = (c.fos_bucket_ok ? 'FOS S3: read/write OK' : 'FOS S3: access failed') + checkedNow;
              const cdnTip = (c.cdn_requests_ok ? 'CDN Edge: responding' : 'CDN Edge: not responding') + checkedNow;

              container.innerHTML =
                '<div class="text-[10px] text-slate-400 font-sans">' +
                  '<div class="truncate">' +
                    '<span class="font-bold text-slate-300 font-sans">DEPLOYED SERVICE: </span>' +
                    '<a href="https://manage.fastly.com/configure/services/' + c.service_id + '" target="_blank" rel="noopener noreferrer" class="text-violet-400 font-semibold font-sans hover:underline" title="Open in Fastly service dashboard">' + c.service_name + ' (' + c.service_id + ')</a>' +
                  '</div>' +
                  '<div class="mt-1 grid grid-cols-3 gap-1 text-[9px] font-sans font-semibold text-center border-t border-slate-800/20 pt-1">' +
                    '<div class="relative group cursor-help p-0.5 bg-black/30 rounded border border-slate-800/50">' + apiDot + ' FASTLY API' +
                      '<span class="absolute left-1/2 -translate-x-1/2 top-full mt-1 z-20 hidden group-hover:block whitespace-nowrap bg-black text-white text-[9px] font-sans font-normal px-2 py-1 rounded shadow-lg border border-slate-700">' + apiTip + '</span>' +
                    '</div>' +
                    '<div class="relative group cursor-help p-0.5 bg-black/30 rounded border border-slate-800/50">' + fosDot + ' FOS S3' +
                      '<span class="absolute left-1/2 -translate-x-1/2 top-full mt-1 z-20 hidden group-hover:block whitespace-nowrap bg-black text-white text-[9px] font-sans font-normal px-2 py-1 rounded shadow-lg border border-slate-700">' + fosTip + '</span>' +
                    '</div>' +
                    '<div class="relative group cursor-help p-0.5 bg-black/30 rounded border border-slate-800/50">' + cdnDot + ' CDN EDGE' +
                      '<span class="absolute left-1/2 -translate-x-1/2 top-full mt-1 z-20 hidden group-hover:block whitespace-nowrap bg-black text-white text-[9px] font-sans font-normal px-2 py-1 rounded shadow-lg border border-slate-700">' + cdnTip + '</span>' +
                    '</div>' +
                  '</div>' +
                '</div>';
            }
          });
        }
      } catch (e) {}
    }

    function checkAllEnvs() {
      envsMapping.forEach((env, i) => {
        setTimeout(async () => {
          const isUp = await pingPort(env.id, env.url);
          if (isUp) {
            updateStats(env.id, env.url);
          } else {
            setDot(env.id, 'containers', false, 'Containers: unknown (port unreachable)');
            renderStatsError(env.id, 'Stats unavailable: port unreachable');
          }
        }, i * 200);
      });
      updateCredentialsStatus();
    }

    logsMapping.forEach(m => updateLogPre(m.id, m.path));
    checkAllEnvs();

    const isProgress = "${status}" === "IN_PROGRESS" || "${status}" === "IN PROGRESS";
    const pollInterval = isProgress ? 1000 : 4000;
    setInterval(() => {
      logsMapping.forEach(m => updateLogPre(m.id, m.path));
    }, pollInterval);
    setInterval(checkAllEnvs, pollInterval);
  </script>
EOF
}

finalize_deploy_report() {
  local status="$1"
  local html_file="$2"
  echo -e "Report index page successfully compiled: ${GREEN}${html_file}${RESET}"

  if [[ "$FLA_BOUNDED_RUN" == "1" ]]; then
    if [[ ( "$NO_TRAFFIC" != "1" || "$OPEN_BROWSER" == "1" ) && "$status" == "IN PROGRESS" && -n "${FLA_PRIVATE_REPORT_URL:-}" && "$OSTYPE" == darwin* ]]; then
      open "$FLA_PRIVATE_REPORT_URL"
    fi
    return 0
  fi

  mkdir -p reports/deploys
  ln -sfn "${DATETIME}" "reports/deploys/current"
  sed 's#<head>#<head>\n  <base href="current/">#' "$html_file" > "reports/deploys/index.html"

  if [[ ( "$NO_TRAFFIC" != "1" || "$OPEN_BROWSER" == "1" ) && "${status}" == "IN PROGRESS" ]]; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
      open "http://localhost:41705/reports/deploys/index.html" || true
    elif [[ "$OSTYPE" == "linux-gnu"* ]] && command -v xdg-open >/dev/null 2>&1; then
      xdg-open "http://localhost:41705/reports/deploys/index.html" || true
    fi
  fi

  mkdir -p reports/deploys
  cd reports/deploys
  ls -dt [0-9][0-9][0-9][0-9]-* 2>/dev/null | tail -n +6 | xargs rm -rf || true
  cd ../..
}

wait_for_remote_image() {
  local image="$1"
  local max_wait="${REGISTRY_WAIT_SECONDS:-900}"
  local interval=10
  local elapsed=0
  local other_errors=0
  local err
  echo -e "\n${BOLD}Checking registry availability for image:${RESET} ${CYAN}$image${RESET} (up to ${max_wait}s)"
  while [ $elapsed -lt $max_wait ]; do
    if err=$(docker manifest inspect "$image" 2>&1 >/dev/null); then
      echo -e "${GREEN}✅ Image is available in the registry! (Confirmed in ${elapsed}s)${RESET}"
      return 0
    fi
    if [[ "$err" == *"no such manifest"* ]]; then
      other_errors=0
      echo -e "   ${YELLOW}- [${elapsed}s] Not published yet (Jenkins build/retag still running)...${RESET}"
    else
      other_errors=$((other_errors + 1))
      echo -e "   ${RED}- [${elapsed}s] Registry lookup error (not a missing tag): ${err}${RESET}"
      if [ $other_errors -ge 6 ]; then
        echo -e "${RED}❌ Registry lookup failed 6 times in a row for $image; check registry login/network.${RESET}"
        return 1
      fi
    fi
    sleep $interval
    elapsed=$((elapsed + interval))
  done
  echo -e "${RED}❌ Timeout after ${max_wait}s waiting for $image to appear in registry (Jenkins build may not have started or finished).${RESET}"
  return 1
}

run_seeded_traffic() {
  local label="$1"
  local url="$2"
  local service_id="$3"
  local backend="$4"
  local log_file="$5"
  local started_at
  local finished_at
  local exit_code

  started_at=$(date +%s)
  echo -e "\n${CYAN}[${label}] Starting traffic seeding${RESET}"
  echo "  Fastly URL: ${url}"
  echo "  Service ID: ${service_id}"
  echo "  Checkpoint backend: ${backend}"
  echo "  Load stages: 500 requests/second for 15 seconds"
  echo "  Max in-flight requests: 20"
  echo "  RUM ratio: 0.30, CMCD ratio: 0.35 (diversified paths/UAs)"
  echo "  Harness log: ${log_file}"

  set +e
  python3 -m scripts.dev.scale_harness run \
    --url "${url}" \
    --service-id "${service_id}" \
    --backend "${backend}" \
    --stages "500:15" \
    --max-in-flight 20 \
    --rum-ratio 0.30 \
    --cmcd-ratio 0.35 \
    --diversify >"${log_file}" 2>&1
  exit_code=$?
  set -e

  finished_at=$(date +%s)
  if [[ "${exit_code}" -eq 0 ]]; then
    echo -e "${GREEN}[${label}] Traffic seeding completed successfully (exit=${exit_code}, elapsed=$((finished_at - started_at))s)${RESET}"
  else
    echo -e "${RED}[${label}] Traffic seeding failed (exit=${exit_code}, elapsed=$((finished_at - started_at))s)${RESET}" >&2
    echo "  Review harness log: ${log_file}" >&2
  fi
  return "${exit_code}"
}

start_continuous_seed() {
  python3 -m scripts.dev.scale_harness run \
    --url "$2" \
    --service-id "$3" \
    --backend "$4" \
    --stages "5:$6" \
    --max-in-flight 10 \
    --rum-ratio 0.30 \
    --cmcd-ratio 0.35 \
    --diversify \
    --settle-seconds 0 >"$5" 2>&1 &
  echo "$!"
}
