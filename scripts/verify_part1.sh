#!/usr/bin/env bash
# ЛР3, кроки 3-5: перевірки після scripts/cluster.sh up. Вивід можна використати як скріншоти для звіту.
set -uo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
if python3 -c "" >/dev/null 2>&1; then PY=python3; else PY=python; fi
CTX="k3d-${CLUSTER:-studies}"
K="kubectl --context $CTX"
BASE="${BASE:-http://localhost:8080}"
PROM="${PROM:-http://localhost:9090}"
J='Content-Type: application/json'
GOOD='{"patient_code":"K8S-001","modality":"CT","slice_count":320,"slice_thickness_mm":0.625,"device":"Siemens SOMATOM","study_date":"2026-03-01"}'
NEW='{"patient_code":"K8S-001","modality":"MRI","slice_count":120,"slice_thickness_mm":3,"device":"GE Signa","study_date":"2026-03-02"}'

hdr() { echo; echo "### $*"; }
call() {  # call <опис> <curl-аргументи...>: код, імʼя пода з X-Pod і початок тіла
  local label="$1"; shift
  local code pod
  code=$(curl -s -D .v_head -o .v_body -w '%{http_code}' "$@")
  pod=$(grep -i '^x-pod:' .v_head | tr -d '\r' | awk '{print $2}')
  printf '%-34s -> %s  pod=%-22s %s\n' "$label" "$code" "${pod:-?}" "$(head -c 150 .v_body)"
}

hdr "Вузли і розміщення подів (db, prometheus, ksm, exporter мають бути на server, web на agent)"
$K get nodes -o wide
$K get pods -A -o wide

hdr "Цикл CRUD через localhost:8080 (NodePort), X-Pod показує под"
call "GET /healthz" "$BASE/healthz"
call "POST /studies" -XPOST "$BASE/studies" -H "$J" -d "$GOOD"
ID=$("$PY" -c "import json;print(json.load(open('.v_body'))['id'])")
call "GET /studies/$ID" "$BASE/studies/$ID"
call "PUT /studies/$ID" -XPUT "$BASE/studies/$ID" -H "$J" -d "$NEW"
call "GET /studies/$ID після PUT" "$BASE/studies/$ID"

hdr "Дані зберігаються після видалення пода бази"
$K -n studies delete pod db-0
$K -n studies wait --for=condition=ready pod/db-0 --timeout=120s
sleep 3
call "GET /studies/$ID після перезапуску бази" "$BASE/studies/$ID"
call "DELETE /studies/$ID" -XDELETE "$BASE/studies/$ID"
call "GET /studies/$ID після видалення" "$BASE/studies/$ID"

hdr "Пароль не в ConfigMap і не в образі (очікується: у ConfigMap жодного DB_PASSWORD)"
$K -n studies get configmap web-config -o yaml | grep -c DB_PASSWORD || true
$K -n studies get deployment web -o jsonpath='{.spec.template.spec.containers[0].env}' ; echo

hdr "Prometheus: цілі"
curl -s "$PROM/api/v1/targets" | "$PY" -c "
import json,sys
for t in json.load(sys.stdin)['data']['activeTargets']:
    print(t['labels']['job'].ljust(20), t['health'].ljust(5), t['labels'].get('pod',t['labels'].get('node',t['labels'].get('instance',''))))"

q() {  # q <опис> <запит>
  echo "--- $1"
  curl -s -G "$PROM/api/v1/query" --data-urlencode "query=$2" | "$PY" -c "
import json,sys
r=json.load(sys.stdin)['data']['result']
if not r: print('  (порожньо)')
for x in r[:8]: print('  ', {k:v for k,v in x['metric'].items() if k in ('pod','container','deployment','horizontalpodautoscaler','state')}, x['value'][1])"
}
hdr "Prometheus: потрібні ряди (кожен пода web окремо; для CFS потрібен хоча б один прогін навантаження або кілька хвилин)"
sleep 15
q "запити за подами"                  'sum by (pod) (http_requests_total)'
q "лічильники CFS: періоди"           'container_cpu_cfs_periods_total{namespace="studies",container="web"}'
q "лічильники CFS: дроселювання"      'container_cpu_cfs_throttled_periods_total{namespace="studies",container="web"}'
q "готові репліки web"                'kube_deployment_status_replicas_ready{namespace="studies",deployment="web"}'
q "М2: зʼєднання з базою за станом"   'pg_conn_count'

hdr "kubectl top (потрібно кілька хвилин після старту кластера)"
$K top nodes
$K top pods -n studies
rm -f .v_head .v_body
