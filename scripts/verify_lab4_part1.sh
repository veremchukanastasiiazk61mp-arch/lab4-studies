#!/usr/bin/env bash
# ЛР4, частина 1 (кроки 1-2): перевірки після scripts/cluster.sh up. Вивід розбитий на блоки ### для скріншотів.
set -uo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
if python3 -c "" >/dev/null 2>&1; then PY=python3; else PY=python; fi
CTX="k3d-${CLUSTER:-studies}"
K="${KUBECTL_CMD:-kubectl --context $CTX}"
BASE="${BASE:-http://localhost:8080}"
PROM="${PROM:-http://localhost:9090}"
J='Content-Type: application/json'
GOOD='{"patient_code":"Q4-001","modality":"CT","slice_count":320,"slice_thickness_mm":0.625,"device":"Siemens SOMATOM","study_date":"2026-03-01"}'
PSQL="$K -n studies exec db-0 -- psql -X -U studies_app -d studies"

hdr() { echo; echo "### $*"; }
call() {  # call <опис> <curl-аргументи...>: код, імʼя пода з X-Pod і початок тіла
  local label="$1"; shift
  local code pod
  code=$(curl -s -D .v_head -o .v_body -w '%{http_code}' "$@")
  pod=$(grep -i '^x-pod:' .v_head | tr -d '\r' | awk '{print $2}')
  printf '%-38s -> %s  pod=%-20s %s\n' "$label" "$code" "${pod:-?}" "$(head -c 170 .v_body)"
}

hdr "Крок 1: схема (таблиці tasks і task_results)"
$PSQL -c '\d tasks'
$PSQL -c '\d task_results'

hdr "Крок 1: ендпоінти черги (створюю запис, задачу для нього і перевіряю відповіді)"
call "POST /studies" -XPOST "$BASE/studies" -H "$J" -d "$GOOD"
SID=$("$PY" -c "import json;print(json.load(open('.v_body'))['id'])")
call "POST /studies/$SID/tasks" -XPOST "$BASE/studies/$SID/tasks"
TID=$("$PY" -c "import json;print(json.load(open('.v_body'))['id'])")
call "POST /studies/999999999/tasks" -XPOST "$BASE/studies/999999999/tasks"
call "GET /tasks/$TID (одразу)" "$BASE/tasks/$TID"
sleep 3
call "GET /tasks/$TID (через 3 с)" "$BASE/tasks/$TID"
call "GET /tasks/999999999" "$BASE/tasks/999999999"
call "GET /tasks?status=done&limit=2" "$BASE/tasks?status=done&limit=2"
call "GET /tasks?status=bad" "$BASE/tasks?status=bad"

hdr "Крок 1: вимірювачі tasks_queued і tasks_running у /metrics сервісу web"
sleep 6
curl -s "$BASE/metrics" | grep -E '^tasks_(queued|running)'

hdr "Крок 2: Deployment worker (без replicas у маніфесті, ресурси, grace period)"
echo "рядків із replicas у k8s/base/40-worker.yaml, не в коментарях: $(grep -v '^ *#' k8s/base/40-worker.yaml | grep -c 'replicas:')"
$K -n studies get deployment worker -o jsonpath='{.spec.template.spec.containers[0].resources}{"\n"}terminationGracePeriodSeconds={.spec.template.spec.terminationGracePeriodSeconds}{"\n"}'
$K -n studies get pods -l app=worker -o wide

hdr "Крок 2: WORKER_ID з Downward API і метрики воркера"
$K -n studies exec deploy/worker -- sh -c 'echo WORKER_ID=$WORKER_ID'
$K -n studies exec deploy/worker -- python -c "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:9100/metrics').read().decode())" | grep -E '^worker_(tasks_total|claims_total|tasks_in_flight|task_duration_seconds_count)'

hdr "Крок 2: поди worker у Prometheus (цілі UP)"
curl -s -G "$PROM/api/v1/query" --data-urlencode 'query=up{job="worker"}' | "$PY" -c "
import json,sys
for r in json.load(sys.stdin)['data']['result']: print(r['metric'].get('pod'), 'up =', r['value'][1])"

hdr "Прибирання (видаляю тестовий запис, задачі видаляються каскадом)"
call "DELETE /studies/$SID" -XDELETE "$BASE/studies/$SID"
call "GET /tasks/$TID після видалення запису" "$BASE/tasks/$TID"
rm -f .v_head .v_body
