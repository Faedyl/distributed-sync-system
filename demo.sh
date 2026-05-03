#!/usr/bin/env bash
# demo.sh — driver script untuk demo Distributed Synchronization System.
# Setiap subcommand memetakan ke satu segmen pada docs/demo_script.md.
#
# Pemakaian: ./demo.sh <subcommand> [opsi]
# Daftar subcommand: ./demo.sh help

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker/docker-compose.yml}"
DEMO_TOPIC="${DEMO_TOPIC:-events}"

# Port → container-name mapping (sesuai docker-compose.yml).
LOCK_PORTS=(18011 18012 18013 18014 18015)
LOCK_NAMES=(lock1 lock2 lock3 lock4 lock5)
QUEUE_PORTS=(18021 18022 18023 18024)
QUEUE_NAMES=(queue1 queue2 queue3 queue4)
CACHE_PORTS=(18031 18032 18033)
CACHE_NAMES=(cache1 cache2 cache3)

c_blue()  { printf '\033[1;34m%s\033[0m\n' "$*"; }
c_green() { printf '\033[1;32m%s\033[0m\n' "$*"; }
c_red()   { printf '\033[1;31m%s\033[0m\n' "$*"; }
c_dim()   { printf '\033[2m%s\033[0m\n' "$*"; }
hr()      { printf '%.0s─' {1..60}; printf '\n'; }

PY=python3
pretty() { "$PY" -m json.tool 2>/dev/null || cat; }

# Resolve pytest: prefer local venv if present.
PYTEST_BIN="pytest"
if [[ -x "$ROOT_DIR/.venv/bin/pytest" ]]; then
  PYTEST_BIN="$ROOT_DIR/.venv/bin/pytest"
fi

_lock_name_for_port() {
  local target=$1 i
  for i in "${!LOCK_PORTS[@]}"; do
    if [[ "${LOCK_PORTS[$i]}" == "$target" ]]; then
      echo "${LOCK_NAMES[$i]}"
      return 0
    fi
  done
  return 1
}

# ──────────────────────────────────────────────────────────────────────────
# Lifecycle
# ──────────────────────────────────────────────────────────────────────────

cmd_up() {
  c_blue "[up] Build & start semua service via Docker Compose"
  docker compose -f "$COMPOSE_FILE" --profile all up -d --build
  c_green "[up] Tunggu cluster konvergen..."
  cmd_wait_leader
}

cmd_down() {
  c_blue "[down] Matikan semua service"
  docker compose -f "$COMPOSE_FILE" --profile all down
}

cmd_ps() {
  docker compose -f "$COMPOSE_FILE" ps
}

cmd_wait_leader() {
  local timeout=${1:-30} elapsed=0 p
  while (( elapsed < timeout )); do
    for p in "${LOCK_PORTS[@]}"; do
      if curl -fsS --max-time 1 "http://127.0.0.1:$p/status" 2>/dev/null \
          | grep -q '"role": "leader"'; then
        c_green "[wait-leader] Leader terpilih pada port $p (setelah ~${elapsed}s)"
        return 0
      fi
    done
    sleep 1
    elapsed=$((elapsed + 1))
  done
  c_red "[wait-leader] Tidak ada leader setelah ${timeout}s — periksa 'docker compose ps'."
  return 1
}

_find_leader_port() {
  local p
  for p in "${LOCK_PORTS[@]}"; do
    if curl -fsS --max-time 1 "http://127.0.0.1:$p/status" 2>/dev/null \
        | grep -q '"role": "leader"'; then
      echo "$p"
      return 0
    fi
  done
  return 1
}

# ──────────────────────────────────────────────────────────────────────────
# Bagian 3.1 — Lock Manager / Raft
# ──────────────────────────────────────────────────────────────────────────

cmd_lock_status() {
  c_blue "[lock-status] Cek role/leader_id/term tiap node"
  local p body
  for p in "${LOCK_PORTS[@]}"; do
    echo "=== port $p ==="
    if body=$(curl -fsS --max-time 2 "http://127.0.0.1:$p/status" 2>/dev/null); then
      "$PY" - "$body" <<'PY'
import json, sys
try:
    d = json.loads(sys.argv[1])
except Exception:
    print("  (parse error)"); sys.exit(0)
r = d.get("raft") or {}
print(f"  node_id   : {d.get('node_id')}")
print(f"  role      : {r.get('role')}")
print(f"  term      : {r.get('term')}")
print(f"  leader_id : {r.get('leader_id')}")
PY
    else
      echo "  (down)"
    fi
  done
}

cmd_lock_acquire() {
  c_blue "[lock-acquire] Dua shared lock + satu exclusive (queued)"
  local leader_port
  if ! leader_port=$(_find_leader_port); then
    c_red "[lock-acquire] Tidak menemukan leader — coba './demo.sh wait-leader' dulu."
    return 1
  fi
  c_dim "→ leader port: $leader_port"
  hr
  c_dim "→ shared reader-1"
  curl -fsS -X POST "http://127.0.0.1:$leader_port/lock/acquire" \
    -H 'content-type: application/json' \
    -d '{"resource":"orders","owner":"reader-1","mode":"shared"}' | pretty
  hr
  c_dim "→ shared reader-2"
  curl -fsS -X POST "http://127.0.0.1:$leader_port/lock/acquire" \
    -H 'content-type: application/json' \
    -d '{"resource":"orders","owner":"reader-2","mode":"shared"}' | pretty
  hr
  c_dim "→ exclusive writer-1 (timeout 2s; akan timeout karena reader masih hold)"
  local ex_body ex_status
  ex_body=$(curl -sS -o /dev/stdout -w 'HTTP_STATUS=%{http_code}' -X POST \
    "http://127.0.0.1:$leader_port/lock/acquire" \
    -H 'content-type: application/json' \
    -d '{"resource":"orders","owner":"writer-1","mode":"exclusive","timeout_ms":2000}')
  ex_status="${ex_body##*HTTP_STATUS=}"
  ex_body="${ex_body%HTTP_STATUS=*}"
  echo "$ex_body" | pretty
  echo "   (http_status=$ex_status — 408 = timeout sesuai harapan)"
  hr
  c_dim "→ release reader-1 + reader-2 supaya state bersih sebelum demo selanjutnya"
  curl -fsS -X POST "http://127.0.0.1:$leader_port/lock/release" \
    -H 'content-type: application/json' \
    -d '{"resource":"orders","owner":"reader-1"}' >/dev/null || true
  curl -fsS -X POST "http://127.0.0.1:$leader_port/lock/release" \
    -H 'content-type: application/json' \
    -d '{"resource":"orders","owner":"reader-2"}' >/dev/null || true
  c_green "[lock-acquire] selesai"
}

cmd_lock_deadlock() {
  c_blue "[lock-deadlock] Jalankan unit test deteksi deadlock"
  ( cd "$ROOT_DIR" && "$PYTEST_BIN" tests/unit/test_lock_state_machine.py::test_deadlock_detection_picks_youngest_victim -v )
}

cmd_lock_failover() {
  c_blue "[lock-failover] Bunuh leader, amati pemilihan baru"
  local leader_port leader_name
  if ! leader_port=$(_find_leader_port); then
    c_red "Tidak ada leader yang ditemukan — apakah cluster sudah jalan?"
    return 1
  fi
  leader_name=$(_lock_name_for_port "$leader_port") || return 1
  echo "Leader saat ini: $leader_name (port $leader_port)"
  echo "→ docker stop $leader_name"
  docker stop "$leader_name" >/dev/null
  echo "$leader_name" > /tmp/.demo_killed_lock
  c_dim "Tunggu election baru (max 15s)..."
  local elapsed=0 p new_leader=""
  while (( elapsed < 15 )); do
    for p in "${LOCK_PORTS[@]}"; do
      [[ "$p" == "$leader_port" ]] && continue
      if curl -fsS --max-time 1 "http://127.0.0.1:$p/status" 2>/dev/null \
          | grep -q '"role": "leader"'; then
        new_leader=$p; break 2
      fi
    done
    sleep 1; elapsed=$((elapsed + 1))
  done
  if [[ -n "$new_leader" ]]; then
    c_green "[lock-failover] Leader baru terpilih dalam ~${elapsed}s di port $new_leader"
  else
    c_red "[lock-failover] Tidak ada leader baru setelah 15s"
  fi
  hr
  cmd_lock_status
  c_dim "Tip: jalankan './demo.sh lock-restore' untuk menghidupkan kembali $leader_name."
}

cmd_lock_restore() {
  if [[ -f /tmp/.demo_killed_lock ]]; then
    local name
    name=$(cat /tmp/.demo_killed_lock)
    docker start "$name" >/dev/null && c_green "[lock-restore] $name online kembali"
    rm -f /tmp/.demo_killed_lock
  else
    echo "Tidak ada container yang tercatat dimatikan."
  fi
}

# ──────────────────────────────────────────────────────────────────────────
# Bagian 3.2 — Distributed Queue
# ──────────────────────────────────────────────────────────────────────────

cmd_queue_topology() {
  c_blue "[queue-topology] Hash ring & placement"
  curl -fsS "http://127.0.0.1:${QUEUE_PORTS[0]}/queue/ring" | pretty
}

cmd_queue_pcap() {
  c_blue "[queue-pcap] Produce 3 pesan, consume, ack"
  local i
  for i in 1 2 3; do
    curl -fsS -X POST "http://127.0.0.1:${QUEUE_PORTS[0]}/queue/produce" \
      -H 'content-type: application/json' \
      -d "{\"topic\":\"$DEMO_TOPIC\",\"payload\":\"order-$i\"}" \
      | "$PY" -c 'import json,sys; d=json.load(sys.stdin); print("   produced seq={} via leader={} replicas={}".format(d.get("seq"), d.get("leader"), d.get("replicas")))'
  done
  hr
  c_dim "→ consume 3 pesan (visibility 5s) sebagai worker-A"
  local resp
  resp=$(curl -fsS -X POST "http://127.0.0.1:${QUEUE_PORTS[0]}/queue/consume" \
    -H 'content-type: application/json' \
    -d "{\"topic\":\"$DEMO_TOPIC\",\"count\":3,\"consumer_id\":\"worker-A\",\"visibility_ms\":5000}")
  echo "$resp" | pretty
  local seqs
  seqs=$(echo "$resp" | "$PY" -c 'import json,sys; d=json.load(sys.stdin); print(" ".join(str(m["seq"]) for m in d.get("messages",[])))')
  hr
  c_dim "→ ack tiap seq: $seqs"
  for s in $seqs; do
    curl -fsS -X POST "http://127.0.0.1:${QUEUE_PORTS[0]}/queue/ack" \
      -H 'content-type: application/json' \
      -d "{\"topic\":\"$DEMO_TOPIC\",\"consumer_id\":\"worker-A\",\"seq\":$s}" \
      | "$PY" -c "import json,sys; d=json.load(sys.stdin); print(f'   ack seq=$s ok={d.get(\"ok\")}')"
  done
  c_green "[queue-pcap] selesai"
}

cmd_queue_visibility() {
  c_blue "[queue-visibility] Worker B ambil tanpa ack → redelivered ke worker C"
  c_dim "→ produce 1 pesan baru"
  curl -fsS -X POST "http://127.0.0.1:${QUEUE_PORTS[0]}/queue/produce" \
    -H 'content-type: application/json' \
    -d "{\"topic\":\"$DEMO_TOPIC\",\"payload\":\"will-be-redelivered\"}" \
    | "$PY" -c 'import json,sys; d=json.load(sys.stdin); print("   produced seq={}".format(d.get("seq")))'
  hr
  c_dim "→ worker-B consume (visibility 2s, TIDAK di-ack)"
  local r1
  r1=$(curl -fsS -X POST "http://127.0.0.1:${QUEUE_PORTS[0]}/queue/consume" \
    -H 'content-type: application/json' \
    -d "{\"topic\":\"$DEMO_TOPIC\",\"count\":1,\"consumer_id\":\"worker-B\",\"visibility_ms\":2000}")
  echo "$r1" | pretty
  c_dim "→ tunggu 3 detik supaya visibility expired..."
  sleep 3
  hr
  c_dim "→ worker-C consume — pesan yang sama harus muncul lagi"
  local r2
  r2=$(curl -fsS -X POST "http://127.0.0.1:${QUEUE_PORTS[0]}/queue/consume" \
    -H 'content-type: application/json' \
    -d "{\"topic\":\"$DEMO_TOPIC\",\"count\":1,\"consumer_id\":\"worker-C\",\"visibility_ms\":5000}")
  echo "$r2" | pretty
  hr
  "$PY" - <<PY
import json
b = json.loads('''$r1''').get("messages", [])
c = json.loads('''$r2''').get("messages", [])
b_seqs = [m["seq"] for m in b]
c_seqs = [m["seq"] for m in c]
print(f"   worker-B got seqs: {b_seqs}")
print(f"   worker-C got seqs: {c_seqs}")
overlap = set(b_seqs) & set(c_seqs)
print(f"   redelivered seqs : {sorted(overlap)}  (harus non-kosong)")
PY
}

cmd_queue_recovery() {
  c_blue "[queue-recovery] Stop & start PRIMARY topik '$DEMO_TOPIC', verifikasi WAL replay"

  # 1) Cari primary untuk DEMO_TOPIC dari hash ring.
  local primary primary_port primary_idx i ring_json
  ring_json=$(curl -fsS "http://127.0.0.1:${QUEUE_PORTS[0]}/queue/ring")
  primary=$("$PY" -c '
import json, sys
ring = json.loads(sys.argv[1])
print(ring.get("primaries", {}).get(sys.argv[2], ""))
' "$ring_json" "$DEMO_TOPIC")
  if [[ -z "$primary" ]]; then
    c_red "Tidak dapat menentukan primary untuk topik '$DEMO_TOPIC'. Pakai topik bawaan ring."
    return 1
  fi
  primary_idx=-1
  for i in "${!QUEUE_NAMES[@]}"; do
    [[ "${QUEUE_NAMES[$i]}" == "$primary" ]] && primary_idx=$i && break
  done
  primary_port="${QUEUE_PORTS[$primary_idx]}"
  echo "→ primary topik '$DEMO_TOPIC' = $primary (port $primary_port)"

  # 2) Pastikan ada minimal 1 pesan di topik supaya next_seq > 0 (bukti WAL).
  curl -fsS -X POST "http://127.0.0.1:$primary_port/queue/produce" \
    -H 'content-type: application/json' \
    -d "{\"topic\":\"$DEMO_TOPIC\",\"payload\":\"pre-crash-marker\"}" \
    | "$PY" -c 'import json,sys; d=json.load(sys.stdin); print("   pre-stop seq={} (akan ada di WAL)".format(d.get("seq")))'

  # 3) Snapshot state sebelum stop.
  c_dim "→ state PRIMARY sebelum stop:"
  curl -fsS "http://127.0.0.1:$primary_port/queue/state" | pretty
  local before_seq state_json
  state_json=$(curl -fsS "http://127.0.0.1:$primary_port/queue/state")
  before_seq=$("$PY" -c '
import json, sys
d = json.loads(sys.argv[1])
ts = d.get("topics", {}).get(sys.argv[2], {})
print(ts.get("next_seq", ""))
' "$state_json" "$DEMO_TOPIC")
  hr

  # 4) Stop & start primary.
  echo "→ docker stop $primary"
  docker stop "$primary" >/dev/null
  sleep 2
  echo "→ docker start $primary"
  docker start "$primary" >/dev/null

  c_dim "→ tunggu $primary boot ulang..."
  local elapsed=0
  while (( elapsed < 20 )); do
    if curl -fsS --max-time 1 "http://127.0.0.1:$primary_port/healthz" >/dev/null 2>&1; then
      break
    fi
    sleep 1; elapsed=$((elapsed + 1))
  done
  c_green "→ $primary online kembali (~${elapsed}s)"

  # 5) Trigger lazy-load topik dengan satu produce — seq baru harus = before_seq.
  hr
  c_dim "→ produce 1 pesan post-recovery: seq baru harus = $before_seq (WAL replayed)"
  curl -fsS -X POST "http://127.0.0.1:$primary_port/queue/produce" \
    -H 'content-type: application/json' \
    -d "{\"topic\":\"$DEMO_TOPIC\",\"payload\":\"post-recovery-marker\"}" \
    | "$PY" -c 'import json,sys; d=json.load(sys.stdin); print("   post-recovery seq={} (lanjut dari WAL)".format(d.get("seq")))'

  hr
  c_dim "→ state setelah recovery (next_seq harus berlanjut, produced bertambah):"
  curl -fsS "http://127.0.0.1:$primary_port/queue/state" | pretty
}

# ──────────────────────────────────────────────────────────────────────────
# Bagian 3.3 — Distributed Cache MESI
# ──────────────────────────────────────────────────────────────────────────

_cache_inspect() {
  local port=$1 key=$2 label=$3
  local resp
  resp=$(curl -fsS "http://127.0.0.1:$port/cache/state")
  echo "$resp" | "$PY" -c "
import json, sys
d = json.load(sys.stdin)
line = d.get('lines', {}).get('$key')
if line is None:
    print(f'   $label key=$key   → (tidak ada line: invalidated atau belum pernah di-cache)')
else:
    print(f'   $label key=$key   → state={line[\"state\"]}  value={line[\"value\"]!r}')
"
}

cmd_cache_mesi() {
  c_blue "[cache-mesi] Write pertama → state Modified"
  curl -fsS -X POST "http://127.0.0.1:${CACHE_PORTS[0]}/cache/set" \
    -H 'content-type: application/json' \
    -d '{"key":"user:42","value":"alice"}' | pretty
  hr
  c_dim "→ inspect state cache1 (harusnya M):"
  _cache_inspect "${CACHE_PORTS[0]}" "user:42" "cache1"
}

cmd_cache_invalidation() {
  c_blue "[cache-invalidation] Read miss (BusRd) → BusRdX → invalidate"
  c_dim "→ pastikan cache1 punya state M dengan menulis ulang user:42"
  curl -fsS -X POST "http://127.0.0.1:${CACHE_PORTS[0]}/cache/set" \
    -H 'content-type: application/json' \
    -d '{"key":"user:42","value":"alice"}' >/dev/null
  hr
  c_dim "→ cache2 GET user:42  (BusRd: cache1 M→S, cache2 I→S)"
  curl -fsS -X POST "http://127.0.0.1:${CACHE_PORTS[1]}/cache/get" \
    -H 'content-type: application/json' \
    -d '{"key":"user:42"}' | pretty
  hr
  _cache_inspect "${CACHE_PORTS[0]}" "user:42" "cache1"
  _cache_inspect "${CACHE_PORTS[1]}" "user:42" "cache2"
  hr
  c_dim "→ cache3 SET user:42 = 'bob'  (BusRdX: invalidate semua peer)"
  curl -fsS -X POST "http://127.0.0.1:${CACHE_PORTS[2]}/cache/set" \
    -H 'content-type: application/json' \
    -d '{"key":"user:42","value":"bob"}' | pretty
  hr
  c_dim "→ inspect ulang setelah invalidasi:"
  _cache_inspect "${CACHE_PORTS[0]}" "user:42" "cache1"
  _cache_inspect "${CACHE_PORTS[1]}" "user:42" "cache2"
  _cache_inspect "${CACHE_PORTS[2]}" "user:42" "cache3"
}

cmd_cache_eviction() {
  c_blue "[cache-eviction] Isi cache melebihi kapasitas → eviction + write-back"
  local n=${1:-1100} i
  c_dim "→ tulis $n key supaya melebihi kapasitas (CACHE_CAPACITY default = 1024)"
  for i in $(seq 1 "$n"); do
    curl -fsS -X POST "http://127.0.0.1:${CACHE_PORTS[0]}/cache/set" \
      -H 'content-type: application/json' \
      -d "{\"key\":\"k$i\",\"value\":\"v$i\"}" >/dev/null
  done
  c_dim "→ counter metrics terkait cache (counters.*):"
  curl -fsS "http://127.0.0.1:${CACHE_PORTS[0]}/metrics.json" \
    | "$PY" -c '
import json, sys
d = json.load(sys.stdin)
counters = d.get("counters", {})
keys_of_interest = (
    "cache_write_with_invalidate", "cache_write_hit_m", "cache_write_hit_e_to_m",
    "cache_evictions", "cache_invalidations",
    "cache_hits", "cache_misses",
    "cache_remote_fill", "cache_memory_fill",
    "cache_snoop_m_downgrade", "cache_snoop_e_downgrade", "cache_snoop_s_serve",
)
for k in keys_of_interest:
    if k in counters:
        print("   {:36s} = {}".format(k, counters[k]))
'
  hr
  c_dim "→ ukuran tabel sekarang (size vs capacity):"
  curl -fsS "http://127.0.0.1:${CACHE_PORTS[0]}/cache/state" \
    | "$PY" -c '
import json, sys
d = json.load(sys.stdin)
print("   capacity = {}, size = {}, policy = {}".format(d.get("capacity"), d.get("size"), d.get("policy")))
'
}

# ──────────────────────────────────────────────────────────────────────────
# Bagian 4 — Performance benchmarks
# ──────────────────────────────────────────────────────────────────────────

cmd_bench() {
  c_blue "[bench] Jalankan skenario benchmark performance"
  ( cd "$ROOT_DIR" && "$PYTEST_BIN" tests/performance/test_benchmark_scenarios.py -v --tb=short )
}

# ──────────────────────────────────────────────────────────────────────────
# Composite runners
# ──────────────────────────────────────────────────────────────────────────

cmd_lock_all()  { cmd_lock_status && cmd_lock_acquire && cmd_lock_deadlock && cmd_lock_failover; }
cmd_queue_all() { cmd_queue_topology && cmd_queue_pcap && cmd_queue_visibility && cmd_queue_recovery; }
cmd_cache_all() { cmd_cache_mesi && cmd_cache_invalidation && cmd_cache_eviction; }

cmd_all() {
  cmd_lock_all
  cmd_lock_restore
  cmd_queue_all
  cmd_cache_all
  cmd_bench
}

# ──────────────────────────────────────────────────────────────────────────
# Help
# ──────────────────────────────────────────────────────────────────────────

cmd_help() {
  cat <<'USAGE'
demo.sh — driver script untuk docs/demo_script.md

Lifecycle:
  up                  Build & jalankan docker compose --profile all (lalu wait-leader)
  down                Matikan semua service
  ps                  Status container
  wait-leader [sec]   Tunggu leader Raft terpilih (default 30s)

Bagian 3.1 — Lock Manager / Raft:
  lock-status         Role/leader_id/term tiap node
  lock-acquire        Shared/shared/exclusive (kompatibilitas mode) — auto-target leader
  lock-deadlock       Pytest: deteksi deadlock
  lock-failover       Bunuh leader, lihat election baru
  lock-restore        Hidupkan kembali node yang dimatikan failover
  lock-all            Jalankan keempat di atas berurutan

Bagian 3.2 — Distributed Queue:
  queue-topology      Inspeksi hash ring & placement (GET /queue/ring)
  queue-pcap          Produce + consume + ack
  queue-visibility    Visibility timeout → at-least-once redelivery
  queue-recovery      Restart primary queue1, cek WAL recovery
  queue-all           Jalankan keempat di atas berurutan

Bagian 3.3 — Distributed Cache MESI:
  cache-mesi          Write pertama → state Modified
  cache-invalidation  BusRd → BusRdX → invalidate semua peer
  cache-eviction [n]  Eviction + write-back (default n=1100 writes)
  cache-all           Jalankan ketiga di atas berurutan

Bagian 4 — Performance:
  bench               Jalankan tests/performance/test_benchmark_scenarios.py

Composite:
  all                 Jalankan semua subcommand demo
  help                Tampilkan pesan ini

Env override:
  COMPOSE_FILE=...    Path docker-compose.yml (default docker/docker-compose.yml)
  DEMO_TOPIC=...      Nama topik untuk queue demo (default 'events')
USAGE
}

# ──────────────────────────────────────────────────────────────────────────
# Dispatcher
# ──────────────────────────────────────────────────────────────────────────

main() {
  local sub="${1:-help}"
  shift || true
  case "$sub" in
    up)                 cmd_up "$@" ;;
    down)               cmd_down "$@" ;;
    ps)                 cmd_ps "$@" ;;
    wait-leader)        cmd_wait_leader "$@" ;;
    lock-status)        cmd_lock_status "$@" ;;
    lock-acquire)       cmd_lock_acquire "$@" ;;
    lock-deadlock)      cmd_lock_deadlock "$@" ;;
    lock-failover)      cmd_lock_failover "$@" ;;
    lock-restore)       cmd_lock_restore "$@" ;;
    lock-all)           cmd_lock_all "$@" ;;
    queue-topology)     cmd_queue_topology "$@" ;;
    queue-pcap)         cmd_queue_pcap "$@" ;;
    queue-visibility)   cmd_queue_visibility "$@" ;;
    queue-recovery)     cmd_queue_recovery "$@" ;;
    queue-all)          cmd_queue_all "$@" ;;
    cache-mesi)         cmd_cache_mesi "$@" ;;
    cache-invalidation) cmd_cache_invalidation "$@" ;;
    cache-eviction)     cmd_cache_eviction "$@" ;;
    cache-all)          cmd_cache_all "$@" ;;
    bench)              cmd_bench "$@" ;;
    all)                cmd_all "$@" ;;
    help|-h|--help)     cmd_help ;;
    *) echo "Subcommand tidak dikenal: $sub"; cmd_help; exit 2 ;;
  esac
}

main "$@"
