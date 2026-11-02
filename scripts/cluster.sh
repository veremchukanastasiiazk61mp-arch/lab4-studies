#!/usr/bin/env bash
# ЛР3 + ЛР4: розгортання і видалення кластера (у ЛР4 додано воркер, його образ і збір його метрик).
#   scripts/cluster.sh up       створити кластер і розгорнути все
#   scripts/cluster.sh down     видалити кластер повністю
#   scripts/cluster.sh delay N  Д6: початкова ініціалізація N секунд (0 або 15)
#   scripts/cluster.sh redeploy зібрати образ заново, завантажити в кластер і перезапустити web
# Параметри через змінні середовища: CLUSTER (studies), STARTUP_DELAY (0 або 15, Д6).
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1   # Git Bash на Windows: не псувати шляхи в аргументах
CLUSTER="${CLUSTER:-studies}"
IMAGE="studies-service:latest"
WORKER_IMAGE="studies-worker:latest"   # ЛР4
CTX="k3d-${CLUSTER}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "Не знайдено '$1'. Встановіть його (див. README_LAB3.md)." >&2; exit 1; }; }

load_password() {
  [ -f .env ] || { echo "Немає файла .env: cp .env.example .env і змініть пароль." >&2; exit 1; }
  DB_PASSWORD="$(grep -E '^DB_PASSWORD=' .env | head -n1 | cut -d= -f2- | tr -d '\r')"
  [ -n "$DB_PASSWORD" ] || { echo "У .env не задано DB_PASSWORD." >&2; exit 1; }
}

apply_secret() {   # Secret з паролем створюється з .env у двох просторах імен
  for ns in studies monitoring; do
    kubectl --context "$CTX" -n "$ns" create secret generic db-credentials \
      --from-literal=DB_PASSWORD="$DB_PASSWORD" --dry-run=client -o yaml | kubectl --context "$CTX" apply -f -
  done
}

build_and_load() {
  docker build -t "$IMAGE" service
  docker build -t "$WORKER_IMAGE" worker
  k3d image import "$IMAGE" "$WORKER_IMAGE" -c "$CLUSTER"
}

up() {
  need docker; need k3d; need kubectl
  load_password
  EXISTED=0
  if k3d cluster list 2>/dev/null | grep -q "^${CLUSTER} "; then
    EXISTED=1
    echo "Кластер ${CLUSTER} уже існує; пропускаю створення (для чистого старту: scripts/cluster.sh down)."
  else
    k3d cluster create --config k3d/cluster.yaml
  fi
  kubectl config use-context "$CTX" >/dev/null

  # мітка role=infra на вузлі server: на ньому працюють база і моніторинг
  kubectl label node "k3d-${CLUSTER}-server-0" role=infra --overwrite

  build_and_load

  kubectl apply -f k8s/base/00-namespaces.yaml
  apply_secret
  kubectl -n studies create configmap db-schema --from-file=init.sql=db/init.sql --dry-run=client -o yaml | kubectl apply -f -
  kubectl -n monitoring create configmap pg-exporter-queries --from-file=queries.yaml=postgres-exporter/queries.yaml --dry-run=client -o yaml | kubectl apply -f -

  # web-config: STARTUP_DELAY_SECONDS береться зі змінної STARTUP_DELAY (за замовчуванням 0)
  mkdir -p .rendered
  sed "s/STARTUP_DELAY_SECONDS: \"0\"/STARTUP_DELAY_SECONDS: \"${STARTUP_DELAY:-0}\"/" k8s/base/20-web.yaml > .rendered/20-web.yaml

  kubectl apply -f k8s/base/10-db.yaml
  kubectl -n studies delete job migrate --ignore-not-found
  kubectl apply -f k8s/base/15-migrate-job.yaml
  kubectl apply -f .rendered/20-web.yaml
  kubectl apply -f k8s/base/30-monitoring.yaml
  kubectl apply -f k8s/base/40-worker.yaml          # ЛР4: пул воркерів (одна репліка, далі kubectl scale)
  # ConfigMap з конфігурацією Prometheus не перечитується сам: перезапуск підхоплює збір метрик воркерів
  kubectl -n monitoring rollout restart deployment/prometheus

  if [ "$EXISTED" = 1 ]; then
    # образ web (ендпоінти черги) оновлено під тим самим тегом: перезапуск підхоплює новий образ у вже існуючому кластері
    # у кластері можуть лишитися HPA і STARTUP_DELAY_SECONDS=15 з ЛР3: у ЛР4 потрібні фіксовані репліки і без ініціалізації
    kubectl -n studies delete hpa web --ignore-not-found
    kubectl -n studies patch configmap web-config --type merge -p '{"data":{"STARTUP_DELAY_SECONDS":"0"}}'
    kubectl -n studies rollout restart deployment/web
  fi

  echo "Чекаю на готовність..."
  kubectl -n studies rollout status statefulset/db --timeout=180s
  kubectl -n studies wait --for=condition=complete job/migrate --timeout=180s
  kubectl -n studies rollout status deployment/web --timeout=180s
  kubectl -n studies rollout status deployment/worker --timeout=180s
  kubectl -n monitoring rollout status deployment/prometheus --timeout=180s
  kubectl -n monitoring rollout status deployment/kube-state-metrics --timeout=180s
  kubectl -n monitoring rollout status deployment/postgres-exporter --timeout=180s
  kubectl get pods -A -o wide
  echo
  echo "Готово: сервіс http://localhost:8080 , Prometheus http://localhost:9090"
}

down() {
  need k3d
  k3d cluster delete "$CLUSTER"
}

redeploy() {
  need docker; need k3d; need kubectl
  build_and_load
  kubectl --context "$CTX" -n studies rollout restart deployment/web
  kubectl --context "$CTX" -n studies rollout status deployment/web --timeout=180s
  kubectl --context "$CTX" -n studies rollout restart deployment/worker
  kubectl --context "$CTX" -n studies rollout status deployment/worker --timeout=180s
}

delay() {   # Д6: scripts/cluster.sh delay 15  (або 0): змінює ConfigMap і перезапускає web
  need kubectl
  local n="${1:?вкажіть кількість секунд}"
  kubectl --context "$CTX" -n studies patch configmap web-config --type merge -p "{\"data\":{\"STARTUP_DELAY_SECONDS\":\"${n}\"}}"
  kubectl --context "$CTX" -n studies rollout restart deployment/web
  kubectl --context "$CTX" -n studies rollout status deployment/web --timeout=180s
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  redeploy) redeploy ;;
  delay) delay "${2:-}" ;;
  *) echo "Використання: $0 up|down|redeploy|delay N" >&2; exit 2 ;;
esac
