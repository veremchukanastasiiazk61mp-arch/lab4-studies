# ЛР4. Синхронізація пула воркерів (варіант 5)

Параметри варіанта 5: q_lo / q_hi = 0,05 / 0,95, T_poll = 10 мс, L = 5 с, сценарій відмови Ф5, додаткова вимога Д2 (`run_after`), R = 5, сутність `studies`, S = 256.

## Що додано до ЛР3

| Частина | Файли |
|---|---|
| Схема: `tasks`, `task_results` (+ `run_after` для Д2) | `db/init.sql`; `db/lab4/naive_results.sql` і `restore_results.sql` перемикають первинний ключ `task_results` (кроки 3 і 4) |
| Сервіс web: `POST /studies/{id}/tasks`, `GET /tasks/{id}`, `GET /tasks`, вимірювачі `tasks_queued`, `tasks_running` | `service/app/main.py`, `service/app/metrics.py` |
| Воркер (окремий образ): три режими захоплення, lease, heartbeat, fencing token, ідемпотентний запис, SIGTERM, FAIL_RATE | `worker/` |
| Deployment `worker` (без `replicas`), збір метрик воркерів у Prometheus | `k8s/base/40-worker.yaml`, `k8s/base/30-monitoring.yaml` |
| Режим створення задач у генераторі | `loadgen/taskgen.py` |
| Усі експерименти кроків 2–9 і Д2 | `scripts/lab4.py` |

## Запуск (Git Bash, корінь проєкту)

Перед початком: Docker Desktop працює, кластер `studies` існує (з ЛР3), у `.env` є пароль.

```
scripts/cluster.sh up
```

Команда на наявному кластері збирає образи web і worker, оновлює схему (Job `migrate`), знімає HPA і початкову ініціалізацію з ЛР3, запускає один воркер. Перевірка:

```
kubectl -n studies get pods
```

### Частина 1 (кроки 1–2): схема, ендпоінти, воркер

```
bash scripts/verify_lab4_part1.sh      # перевірки для скріншотів: схема, ендпоінти, метрики, Deployment worker
python scripts/lab4.py prep            # 100 записів для генератора, перевірка POST /studies/{id}/tasks
python scripts/lab4.py step2           # 20 задач на одному воркері: t_io і t_cpu          (~1 хв)
```

### Наступні частини

Далі кроки по черзі. Кожен записує сирі результати в `results/lab4/` і друкує підсумок; після кожного кроку збережіть вивід терміналу.

```
python scripts/lab4.py prep       # 100 записів для генератора, перевірка ендпоінта задач
python scripts/lab4.py step2      # 20 задач на одному воркері: t_io і t_cpu          (~1 хв)
python scripts/lab4.py step3      # наївне захоплення: D для T_poll = 10 і 1 мс       (~3 хв)
python scripts/lab4.py step4      # SKIP LOCKED і FOR UPDATE                          (~4 хв)
python scripts/lab4.py step5      # lease, heartbeat, fencing, kill -STOP / -CONT     (~3 хв)
python scripts/lab4.py step6      # X(W) для W = 1, 2, 5                              (~10 хв)
python scripts/lab4.py step7      # закон Літтла: 0,7 і 1,3 від X(R)                  (~7 хв)
python scripts/lab4.py step8      # SIGTERM: rollout restart під навантаженням        (~4 хв)
python scripts/lab4.py step9      # FAIL_RATE = 0,15 і сценарій Ф5 (три прогони)      (~25 хв)
python scripts/lab4.py d2         # Д2: run_after                                     (~2 хв)
```

Порядок важливий: `step6` зберігає X(R) і t_cpu для кроків 7–9 (`results/lab4/state.json`).

Під час прогонів не запускайте важких програм: кластер, генератор і Prometheus ділять процесор ноутбука.

Якщо `kubectl` не підключається (як у ЛР3): `export KUBECTL_CMD="docker exec -i k3d-studies-server-0 kubectl"`.

Для налагодження можна скоротити всі тривалості: `--time-scale 0.2` (лише для перевірки, у звіт такі прогони не йдуть).

## Аналіз

```
pip install matplotlib
python analysis/analyze_lab4.py
```

Створює `results/lab4/analysis_lab4.json`, `analysis_lab4.md` (таблиці для звіту) і рисунки в `report/lab4/`.

## Примітка про `kill -STOP 1`

Команда з завдання (`kubectl exec ... -- kill -STOP 1`) у контейнері не зупиняє процес: ядро Linux ігнорує SIGSTOP для PID 1 зсередини його власного простору процесів. Скрипт кроку 5 спершу виконує команду із завдання, перевіряє стан `/proc/1/status` (результат записується в `step5.json`), і якщо процес не зупинився, надсилає той самий сигнал тому самому процесу з вузла k3d (`docker exec k3d-studies-agent-N ...`).

## Примітка про використання моделі

Код і скрипти цієї роботи підготовлено з використанням Claude (Anthropic); усі запуски і вимірювання виконує автор роботи на своєму комп'ютері.
