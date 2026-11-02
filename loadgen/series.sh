#!/usr/bin/env bash
# Уся серія вимірювань: 1 і 2 вебінстанси × рівні 10/100/500 rps × 3 повтори,
# самоперевірка генератора (/noop) і прогін закритого контуру.
#   bash loadgen/series.sh
# Параметри середовища (необовʼязкові): BASE, RATES, REPS, WARMUP, DURATION, TIMEOUT, CLOSED_RATE
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1   # Git Bash на Windows: не псувати шляхи на кшталт /app у docker
if python3 -c "" >/dev/null 2>&1; then PY=python3; else PY=python; fi

BASE="${BASE:-http://localhost:8080}"
RATES="${RATES:-10 100 500}"
REPS="${REPS:-3}"
WARMUP="${WARMUP:-10}"
DURATION="${DURATION:-30}"
TIMEOUT="${TIMEOUT:-5}"
CLOSED_RATE="${CLOSED_RATE:-500}"   # рівень, на якому 1 інстанс уже не тримає навантаження

[ -f .env ] || { echo "Створіть .env: cp .env.example .env"; exit 1; }
mkdir -p results/raw
{ echo "date: $(date -Is)"; echo "cpu_count: $(nproc)"; uname -a; docker version --format 'docker {{.Server.Version}}'; docker compose version; } > results/env.txt

wait_healthy() {
  for _ in $(seq 1 120); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/healthz" || true)" = "200" ] && return 0
    sleep 1
  done
  echo "стенд не став готовим за 120 с" >&2; exit 1
}

gen() { "$PY" loadgen/loadgen.py --url "$BASE" --warmup "$WARMUP" --duration "$DURATION" --timeout "$TIMEOUT" "$@"; }

for n in 1 2; do
  echo "=== $n вебінстанс(ів) ==="
  docker compose down -v >/dev/null 2>&1 || true
  WEB_REPLICAS=$n docker compose up -d --build
  wait_healthy
  "$PY" loadgen/seed.py --url "$BASE" --count 100 --out loadgen/ids.json

  # Самоперевірка генератора: ендпоінт, який нічого не робить
  gen --noop --rate "$CLOSED_RATE" --name "noop_n${n}_r${CLOSED_RATE}"

  for rate in $RATES; do
    for rep in $(seq 1 "$REPS"); do
      gen --rate "$rate" --name "open_n${n}_r${rate}_rep${rep}"
    done
  done

  # Підтвердження розподілу на боці балансувальника
  docker compose logs --no-log-prefix lb > "results/lb_n${n}.log" 2>&1 || true

  if [ "$n" = "1" ]; then
    C="$("$PY" loadgen/summarize.py --concurrency 1 "$CLOSED_RATE")"
    echo "=== закритий контур, конкурентність C=$C (медіана L відкритого контуру) ==="
    for rep in $(seq 1 "$REPS"); do
      gen --mode closed --concurrency "$C" --name "closed_n1_r${CLOSED_RATE}_c${C}_rep${rep}"
    done
  fi
done

"$PY" loadgen/summarize.py > /dev/null
echo "Готово. Зведення: results/summary.md"
