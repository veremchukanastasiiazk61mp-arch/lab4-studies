#!/usr/bin/env python3
"""ЛР4: експерименти з чергою задач і пулом воркерів. Усі кроки виконуються цим скриптом, кожен записує сирі
результати в results/lab4/<крок>.json; таблиці і діаграми будує analysis/analyze_lab4.py.

Запуск з кореня проєкту (Git Bash), кластер має працювати (scripts/cluster.sh up):
  python scripts/lab4.py prep        # 100 записів для генератора, перевірка стенда
  python scripts/lab4.py step2       # 20 задач на одному воркері: t_io, t_cpu
  python scripts/lab4.py step3       # наївне захоплення: D для T_poll = 10 і 1 мс
  python scripts/lab4.py step4       # SKIP LOCKED і FOR UPDATE
  python scripts/lab4.py step5       # lease, heartbeat, fencing: kill -STOP / -CONT
  python scripts/lab4.py step6       # X(W) для W = 1, 2, 5
  python scripts/lab4.py step7       # закон Літтла: 0,7 і 1,3 від X(R)
  python scripts/lab4.py step8       # SIGTERM: rollout restart під навантаженням
  python scripts/lab4.py step9       # FAIL_RATE = 0,15 і сценарій Ф5
  python scripts/lab4.py d2          # Д2: run_after
Параметри часу можна зменшити для налагодження: --time-scale 0.1 (усі тривалості множаться на це число).
Якщо kubectl з Windows не підключається: KUBECTL_CMD="docker exec -i k3d-studies-server-0 kubectl".
"""
import argparse
import json
import math
import os
import re
import shlex
import statistics
import subprocess
import sys
import threading
import time
import urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RES = os.path.join(ROOT, "results", "lab4")
PY = sys.executable
CLUSTER = os.environ.get("CLUSTER", "studies")
CTX = f"k3d-{CLUSTER}"
BASE = os.environ.get("WEB_BASE", "http://localhost:8080")
PROM = os.environ.get("PROM_URL", "http://localhost:9090")
R = 5                      # найбільша кількість воркерів за варіантом (R з лабораторної роботи 3)
POLL_MS = 10               # T_poll варіанта 5
LEASE_S = 5.0              # L варіанта 5
TIME_SCALE = 1.0


def T(x):
    return x * TIME_SCALE


# ------------------------------------------------------------------ середовище (Kubernetes)

class K8s:
    """Усе, що скрипт робить із кластером. Окремий клас, щоб локальну перевірку без кластера можна було
    виконати з підміною (див. worker/tests/local_env.py)."""

    def __init__(self):
        custom = os.environ.get("KUBECTL_CMD")
        self.kc = shlex.split(custom) if custom else ["kubectl", "--context", CTX]

    def kubectl(self, *args, check=True, input=None, timeout=300):
        r = subprocess.run([*self.kc, *args], cwd=ROOT, encoding="utf-8", errors="replace", capture_output=True, input=input, timeout=timeout)
        if check and r.returncode != 0:
            raise RuntimeError(f"kubectl {' '.join(args)}: {r.stderr.strip()}")
        return r.stdout

    def sql(self, q):
        out = self.kubectl("-n", "studies", "exec", "db-0", "--", "psql", "-X", "-At", "-F", "|",
                           "-U", "studies_app", "-d", "studies", "-c", q)
        return out.strip()

    def sql_file(self, path):
        text = open(path, encoding="utf-8").read()
        self.kubectl("-n", "studies", "exec", "-i", "db-0", "--", "psql", "-X", "-q", "-v", "ON_ERROR_STOP=1",
                     "-U", "studies_app", "-d", "studies", input=text)

    def pods(self):
        """Поди worker без тих, що видаляються: [(імʼя, готовий, перезапуски)]."""
        data = json.loads(self.kubectl("-n", "studies", "get", "pods", "-l", "app=worker", "-o", "json"))
        out = []
        for p in data["items"]:
            if p["metadata"].get("deletionTimestamp"):
                continue
            st = p.get("status", {})
            ready = any(c["type"] == "Ready" and c["status"] == "True" for c in st.get("conditions", []))
            restarts = sum(c.get("restartCount", 0) for c in st.get("containerStatuses", []))
            out.append((p["metadata"]["name"], ready, restarts))
        return out

    def all_pods_json(self):
        return json.loads(self.kubectl("-n", "studies", "get", "pods", "-l", "app=worker", "-o", "json"))

    def wait_workers(self, n, seconds=180):
        for _ in range(seconds):
            try:
                pods = self.pods()
            except Exception:  # noqa: BLE001
                pods = []
            if len(pods) == n and all(r for _, r, _ in pods):
                return [p for p, _, _ in pods]
            time.sleep(1)
        raise RuntimeError(f"не дочекались {n} готових подів worker")

    def scale(self, n):
        self.kubectl("-n", "studies", "scale", "deployment/worker", f"--replicas={n}")
        return self.wait_workers(n)

    def set_env(self, n, **kv):
        """Змінює змінні Deployment (це перезапускає поди) і чекає n готових подів."""
        args = [f"{k}={v}" for k, v in kv.items()]
        self.kubectl("-n", "studies", "scale", "deployment/worker", f"--replicas={n}")
        self.kubectl("-n", "studies", "set", "env", "deployment/worker", *args)
        self.kubectl("-n", "studies", "rollout", "status", "deployment/worker", "--timeout=240s", timeout=300)
        return self.wait_workers(n)

    def rollout_restart(self):
        self.kubectl("-n", "studies", "rollout", "restart", "deployment/worker")

    def scrape(self, pod):
        code = "import urllib.request;print(urllib.request.urlopen('http://127.0.0.1:9100/metrics').read().decode())"
        return parse_metrics(self.kubectl("-n", "studies", "exec", pod, "--", "python", "-c", code))

    def _node_exec(self, pod, cmd):
        """Читає журнал з тому emptyDir на вузлі k3d: працює і тоді, коли контейнер пода не запущено (пауза між
        перезапусками). Том пода лежить у /var/lib/kubelet/pods/<uid>/volumes/kubernetes.io~empty-dir/logs."""
        d = json.loads(self.kubectl("-n", "studies", "get", "pod", pod, "-o", "json"))
        path = f"/var/lib/kubelet/pods/{d['metadata']['uid']}/volumes/kubernetes.io~empty-dir/logs/worker.log"
        r = subprocess.run(["docker", "exec", d["spec"]["nodeName"], "sh", "-c",
                            cmd.replace("/var/log/worker/worker.log", path)],
                           encoding="utf-8", errors="replace", capture_output=True, timeout=60)
        if r.returncode != 0:
            raise RuntimeError(r.stderr.strip())
        return r.stdout

    unreadable = []

    def _exec_log(self, pod, cmd):
        """Читає журнал пода воркера: спершу kubectl exec, а якщо контейнер саме перезапускається (FAIL_RATE),
        то з вузла; кілька повторів із паузами."""
        err = None
        for _ in range(5):
            for reader in (lambda: self.kubectl("-n", "studies", "exec", pod, "--", "sh", "-c", cmd),
                           lambda: self._node_exec(pod, cmd)):
                try:
                    return reader()
                except Exception as exc:  # noqa: BLE001
                    err = exc
            time.sleep(3)
        print(f"   УВАГА: журнал пода {pod} не прочитано ({err}); його рядки не враховано", file=sys.stderr)
        self.unreadable.append(pod)
        return ""

    def log_grep_count(self, pod, pattern):
        out = self._exec_log(pod, f"grep -cF -- {shlex.quote(pattern)} /var/log/worker/worker.log || true")
        return int(out.strip() or 0)

    def log_grep(self, pod, pattern):
        out = self._exec_log(pod, f"grep -F -- {shlex.quote(pattern)} /var/log/worker/worker.log || true")
        return out.splitlines()

    # Сигнали SIGSTOP/SIGCONT процесу з PID 1. Команда із завдання (kubectl exec ... -- kill -STOP 1) у цьому
    # образі НЕ діє: ядро Linux ігнорує SIGSTOP, надісланий PID 1 зсередини його власного простору PID (процес init
    # контейнера захищений). Тому спершу пробуємо команду із завдання і перевіряємо стан /proc/1/status; якщо вона не
    # зупинила процес, той самий сигнал надсилається з вузла k3d (він є предком простору PID контейнера, і сигнал
    # доходить). Результат перевірки зберігається в self.pid1_probe для звіту.
    pid1_probe = None
    _node_of = {}

    def _pid1_state(self, pod):
        out = self.kubectl("-n", "studies", "exec", pod, "--", "sh", "-c", "grep '^State' /proc/1/status").strip()
        return out

    def _node_signal(self, pod, sig):
        if pod not in self._node_of:
            self._node_of[pod] = self.kubectl("-n", "studies", "get", "pod", pod, "-o", "jsonpath={.spec.nodeName}").strip()
        node = self._node_of[pod]
        script = (f"for f in /proc/[0-9]*/environ; do if ( tr '\\0' '\\n' < $f ) 2>/dev/null | grep -qx 'WORKER_ID={pod}'; "
                  f"then p=${{f#/proc/}}; p=${{p%/environ}}; kill -{sig} $p; fi; done")
        r = subprocess.run(["docker", "exec", node, "sh", "-c", script], encoding="utf-8", errors="replace", capture_output=True, timeout=60)
        if r.returncode != 0:
            raise RuntimeError(f"docker exec {node}: {r.stderr.strip()}")

    def signal(self, pod, sig):
        if self.pid1_probe is None:
            before = self._pid1_state(pod)
            self.kubectl("-n", "studies", "exec", pod, "--", "kill", f"-{sig}", "1", check=False)
            time.sleep(0.5)
            after = self._pid1_state(pod)
            works = (sig == "STOP") == after.split(":", 1)[1].strip().startswith("T")
            self.pid1_probe = {"command": f"kubectl exec {pod} -- kill -{sig} 1", "state_before": before,
                               "state_after": after, "works": works}
            print("   перевірка kill -%s 1 зсередини контейнера: %s -> %s; %s" % (
                sig, before, after, "діє" if works else "НЕ діє, сигнал надсилається з вузла"))
            if works:
                return
            self._node_signal(pod, sig)
            return
        if self.pid1_probe["works"]:
            self.kubectl("-n", "studies", "exec", pod, "--", "kill", f"-{sig}", "1")
        else:
            self._node_signal(pod, sig)

    def delete_pod_force(self, pod):
        self.kubectl("-n", "studies", "delete", "pod", pod, "--grace-period=0", "--force")

    def follow_logs(self, pod, path):
        f = open(path, "w", encoding="utf-8")
        p = subprocess.Popen([*self.kc, "-n", "studies", "logs", "-f", pod], stdout=f, stderr=subprocess.STDOUT, cwd=ROOT)
        return p, f

    def watch_pods(self, path):
        """Крок 8: стежить за подами worker (kubectl get pods -w -o json) і пише кожну зміну стану у файл; так
        ловляться коди завершення контейнерів, які зникають разом із подом."""
        f = open(path, "w", encoding="utf-8")
        p = subprocess.Popen([*self.kc, "-n", "studies", "get", "pods", "-l", "app=worker", "-w", "-o", "json"],
                             stdout=f, stderr=subprocess.DEVNULL, cwd=ROOT)
        return p, f


ENV = K8s()


# ------------------------------------------------------------------ допоміжне

def parse_metrics(text):
    """{(імʼя, (('мітка','значення'),...)): float} для текстового формату Prometheus."""
    out = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)", line)
        if not m:
            continue
        labels = tuple(sorted(re.findall(r'(\w+)="([^"]*)"', m.group(2) or "")))
        out[(m.group(1), labels)] = float(m.group(3))
    return out


def parse_exit_codes(path):
    """З файла kubectl get pods -w -o json (послідовність JSON-обʼєктів): {под: {exitCode, reason}} для контейнерів,
    що завершились (стан terminated)."""
    out = {}
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except FileNotFoundError:
        return out
    dec, i = json.JSONDecoder(), 0
    while True:
        while i < len(text) and text[i].isspace():
            i += 1
        if i >= len(text):
            break
        try:
            obj, i = dec.raw_decode(text, i)
        except ValueError:
            break
        for cs in obj.get("status", {}).get("containerStatuses", []):
            term = cs.get("state", {}).get("terminated") or cs.get("lastState", {}).get("terminated")
            if term:
                out[obj["metadata"]["name"]] = {"exitCode": term.get("exitCode"), "reason": term.get("reason")}
    return out


def metric(snap, name, **labels):
    want = tuple(sorted(labels.items()))
    return snap.get((name, want), 0.0)


def snapshot_workers(pods):
    return {p: ENV.scrape(p) for p in pods}


def diff_hist(before, after, step):
    """Різниця кумулятивних бакетів worker_task_duration_seconds{step} між двома знімками, сума за подами:
    [(межа, кількість)] за зростанням і загальна кількість."""
    buckets = {}
    total = 0.0
    for pod, snap in after.items():
        b0 = before.get(pod, {})
        for (name, labels), v in snap.items():
            if name != "worker_task_duration_seconds_bucket":
                continue
            d = dict(labels)
            if d.get("step") != step:
                continue
            le = float("inf") if d["le"] == "+Inf" else float(d["le"])
            buckets[le] = buckets.get(le, 0.0) + v - b0.get((name, labels), 0.0)
    items = sorted(buckets.items())
    total = items[-1][1] if items else 0.0
    return items, total


def quantile_from_buckets(items, total, q):
    """Верхня межа бакета, у який потрапляє квантиль q (як оцінка за гістограмою Г3: точність до ширини бакета)."""
    if total <= 0:
        return None
    need = q * total
    for le, c in items:
        if c >= need:
            return le
    return None


def diff_counter(before, after, name, **labels):
    want = tuple(sorted(labels.items()))
    return sum(snap.get((name, want), 0.0) - before.get(p, {}).get((name, want), 0.0) for p, snap in after.items())


def sum_counter_total(after, name, **labels):
    want = tuple(sorted(labels.items()))
    return sum(snap.get((name, want), 0.0) for snap in after.values())


def http_get(url, timeout=3):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


SAMPLE_SQL = (
    "SELECT extract(epoch FROM now()), "
    "(SELECT count(*) FROM tasks WHERE status = 'queued'), (SELECT count(*) FROM tasks WHERE status = 'running'), "
    "(SELECT count(*) FROM tasks WHERE status = 'done'), (SELECT count(*) FROM tasks WHERE status = 'failed'), "
    "(SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = 'studies'), "
    "(SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock')"
)


class Sampler(threading.Thread):
    """Кожну секунду знімає стан бази (черга, виконання, транзакції, очікування блокувань) і, раз на 2 с,
    вимірювач tasks_queued із /metrics сервісу web. Потрібен для діаграм і для кроків 4, 6, 7, 9."""

    def __init__(self, with_gauge=False):
        super().__init__(daemon=True)
        self.rows = []
        self.gauge = []
        self.with_gauge = with_gauge
        self._halt = threading.Event()

    def run(self):
        last_g = 0.0
        while not self._halt.is_set():
            t0 = time.time()
            try:
                v = ENV.sql(SAMPLE_SQL).split("|")
                self.rows.append({"t": float(v[0]), "queued": int(v[1]), "running": int(v[2]), "done": int(v[3]),
                                  "failed": int(v[4]), "xact": int(v[5]), "lock_waits": int(v[6])})
            except Exception as exc:  # noqa: BLE001
                self.rows.append({"t": time.time(), "error": str(exc)[:120]})
            if self.with_gauge and time.time() - last_g >= 2:
                last_g = time.time()
                try:
                    m = parse_metrics(http_get(f"{BASE}/metrics"))
                    self.gauge.append({"t": last_g, "tasks_queued": m.get(("tasks_queued", ()), None),
                                       "tasks_running": m.get(("tasks_running", ()), None)})
                except Exception:  # noqa: BLE001
                    pass
            self._halt.wait(max(0.0, 1.0 - (time.time() - t0)))

    def stop(self):
        self._halt.set()
        self.join(timeout=10)


def taskgen(out, rate, count=None, duration=None, delay=None, wait=True):
    cmd = [PY, os.path.join(ROOT, "loadgen", "taskgen.py"), "--url", BASE, "--ids", os.path.join(ROOT, "loadgen", "ids.json"),
           "--rate", str(rate), "--out", out]
    cmd += ["--count", str(count)] if count is not None else ["--duration", str(duration)]
    if delay:
        cmd += ["--delay-min", str(delay[0]), "--delay-max", str(delay[1])]
    p = subprocess.Popen(cmd, cwd=ROOT, encoding="utf-8", errors="replace", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if not wait:
        return p
    o = p.communicate()[0]
    print("   ", o.strip())
    return json.load(open(out))["summary"]


def wait_drain(timeout):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if ENV.sql("SELECT count(*) FROM tasks WHERE status IN ('queued', 'running')") == "0":
            return True
        time.sleep(1)
    return False


def reset_tasks():
    ENV.sql("TRUNCATE tasks, task_results RESTART IDENTITY")


def dup_stats():
    """n_dup за таблицею результатів: задачі з більш ніж одним рядком."""
    n = int(ENV.sql("SELECT count(*) FROM tasks"))
    dup = int(ENV.sql("SELECT count(*) FROM (SELECT task_id FROM task_results GROUP BY 1 HAVING count(*) > 1) x"))
    total_results = int(ENV.sql("SELECT count(*) FROM task_results"))
    return n, dup, total_results


def dup_from_logs(pods):
    """n_dup за журналами: задачі, позначені як виконані більше одного разу (рядки «задача N виконано»)."""
    seen = {}
    for p in pods:
        for line in ENV.log_grep(p, "[task_done]"):
            m = re.search(r"\[task_done\]\D*(\d+)", line)
            if m:
                seen[m.group(1)] = seen.get(m.group(1), 0) + 1
    return sum(1 for c in seen.values() if c > 1), len(seen)


def pctl(sorted_vals, q):
    if not sorted_vals:
        return None
    return sorted_vals[min(len(sorted_vals) - 1, int(math.ceil(q * len(sorted_vals))) - 1)]


def load_state():
    p = os.path.join(RES, "state.json")
    return json.load(open(p)) if os.path.exists(p) else {}


def save_state(**kv):
    st = load_state()
    st.update(kv)
    json.dump(st, open(os.path.join(RES, "state.json"), "w"), indent=2)


def save(name, obj):
    path = os.path.join(RES, name + ".json")
    json.dump(obj, open(path, "w"), indent=1, ensure_ascii=False)
    print(f"   записано {os.path.relpath(path, ROOT)}")


def default_env(workers, **over):
    """Типові значення воркера варіанта 5 (T_poll, L, skip locked, heartbeat, без падінь); over перевизначає."""
    kv = {"CLAIM_MODE": "skip_locked", "POLL_INTERVAL_MS": POLL_MS, "LEASE_SECONDS": LEASE_S, "HEARTBEAT": "true",
          "FAIL_RATE": "0"}
    kv.update(over)
    return ENV.set_env(workers, **kv)


def per_second_done(t_from, t_to):
    """Кількість завершених задач за секунду (за finished_at у базі) у вікні [t_from, t_to] (unix-час)."""
    out = ENV.sql("SELECT floor(extract(epoch FROM finished_at))::bigint, count(*) FROM tasks "
                  f"WHERE status = 'done' AND finished_at >= to_timestamp({t_from}) AND finished_at < to_timestamp({t_to}) "
                  "GROUP BY 1 ORDER BY 1")
    d = {}
    for line in out.splitlines():
        a, b = line.split("|")
        d[int(a)] = int(b)
    secs = list(range(int(math.ceil(t_from)), int(math.floor(t_to))))
    return secs, [d.get(s, 0) for s in secs]


# ------------------------------------------------------------------ кроки

def cmd_prep(a):
    ids_path = os.path.join(ROOT, "loadgen", "ids.json")
    ok = False
    if os.path.exists(ids_path):
        ids = json.load(open(ids_path))
        ok = len(ids) >= 100
        if ok:
            try:
                http_get(f"{BASE}/studies/{ids[0]}")
                http_get(f"{BASE}/studies/{ids[-1]}")
            except Exception:  # noqa: BLE001
                ok = False
    if not ok:
        subprocess.run([PY, os.path.join(ROOT, "loadgen", "seed.py"), "--url", BASE, "--count", "100", "--out", ids_path],
                       check=True, cwd=ROOT)
    print("tasks:", ENV.sql("SELECT count(*) FROM tasks"), "task_results:", ENV.sql("SELECT count(*) FROM task_results"))
    print("перевіряю POST /studies/{id}/tasks ...")
    ids = json.load(open(ids_path))
    req = urllib.request.Request(f"{BASE}/studies/{ids[0]}/tasks", data=b"", method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        print("   ", r.status, r.read().decode()[:200])
    ENV.sql("TRUNCATE tasks, task_results RESTART IDENTITY")
    print("стенд готовий")


def cmd_step2(a):
    """20 задач на одному воркері: t_io (fetch + store) і t_cpu (process) як середні за гістограмою."""
    pods = default_env(1)
    reset_tasks()
    b = snapshot_workers(pods)
    s = taskgen(os.path.join(RES, "step2_tasks.json"), rate=2, count=20)
    ok = wait_drain(T(120))
    af = snapshot_workers(pods)
    res = {"drained": ok, "tasks": s["created"]}
    for st in ("claim", "fetch", "process", "store"):
        n = diff_counter(b, af, "worker_task_duration_seconds_count", step=st)
        sm = diff_counter(b, af, "worker_task_duration_seconds_sum", step=st)
        res[st] = {"count": n, "mean_ms": 1000 * sm / n if n else None}
    res["t_io_ms"] = res["fetch"]["mean_ms"] + res["store"]["mean_ms"]
    res["t_cpu_ms"] = res["process"]["mean_ms"]
    res["done"] = int(ENV.sql("SELECT count(*) FROM tasks WHERE status = 'done'"))
    print(f"   t_io = {res['t_io_ms']:.1f} мс, t_cpu = {res['t_cpu_ms']:.1f} мс; виконано {res['done']}/20")
    save("step2", res)
    save_state(t_io_ms=res["t_io_ms"], t_cpu_ms=res["t_cpu_ms"])


def run_series(label, mode, poll_ms, tasks=300, rate=100, sample=False):
    pods = ENV.set_env(R, CLAIM_MODE=mode, POLL_INTERVAL_MS=poll_ms, LEASE_SECONDS=LEASE_S, HEARTBEAT="true", FAIL_RATE="0")
    reset_tasks()
    time.sleep(2)
    before = snapshot_workers(pods)
    sampler = Sampler()
    sampler.start()
    t0 = time.time()
    s = taskgen(os.path.join(RES, f"{label}_tasks.json"), rate=rate, count=tasks)
    drained = wait_drain(T(300))
    t1 = time.time()
    sampler.stop()
    after = snapshot_workers(pods)
    n, dup, total_res = dup_stats()
    dup_log, seen_log = dup_from_logs(pods)
    items, total = diff_hist(before, after, "claim")
    res = {"label": label, "mode": mode, "poll_ms": poll_ms, "tasks": n, "created": s["created"], "drained": drained,
           "duration_s": t1 - t0, "results_rows": total_res, "n_dup_results": dup, "D_results": dup / n if n else None,
           "n_dup_logs": dup_log, "D_logs": dup_log / n if n else None, "tasks_seen_in_logs": seen_log,
           "claim_p99_s": quantile_from_buckets(items, total, 0.99), "claim_count": total,
           "empty_claims": diff_counter(before, after, "worker_claims_total", result="empty"),
           "task_claims": diff_counter(before, after, "worker_claims_total", result="task"),
           "max_lock_waits": max((r.get("lock_waits", 0) for r in sampler.rows), default=0),
           "failed": int(ENV.sql("SELECT count(*) FROM tasks WHERE status = 'failed'"))}
    print(f"   {label}: D за результатами {res['D_results']:.3f}, за журналами {res['D_logs']:.3f}, "
          f"p99 захоплення {res['claim_p99_s']}, порожніх захоплень {res['empty_claims']:.0f}, "
          f"макс. очікувань Lock {res['max_lock_waits']}")
    save(label, res)
    return res


def cmd_step3(a):
    ENV.sql_file(os.path.join(ROOT, "db", "lab4", "naive_results.sql"))   # PK task_results -> звичайний індекс
    run_series("step3_naive_poll10", "naive", 10)
    run_series("step3_naive_poll1", "naive", 1)


def cmd_step4(a):
    ENV.sql_file(os.path.join(ROOT, "db", "lab4", "restore_results.sql"))  # PK task_results повернуто
    run_series("step4_skip_locked_poll1", "skip_locked", 1)
    run_series("step4_for_update_poll1", "for_update", 1)
    print("для подальших кроків: SKIP LOCKED і T_poll 10 мс (default_env робить це автоматично)")


def cmd_step5(a):
    """Lease, heartbeat, fencing на двох воркерах: один зупиняється на 2L секунд (kill -STOP 1), далі kill -CONT 1."""
    ENV.sql_file(os.path.join(ROOT, "db", "lab4", "restore_results.sql"))
    pods = default_env(2)
    reset_tasks()
    p = taskgen(os.path.join(RES, "step5_tasks.json"), rate=10, count=100, wait=False)
    victim = None
    task = None
    t_wait = time.time()
    # Задача триває десятки мілісекунд, а kubectl/docker exec — сотні, тому ловити задачу «вчасно» неможливо:
    # зупиняємо воркер, і лише потім дивимось, чи тримає він задачу в статусі running; якщо ні, відновлюємо і пробуємо знову.
    tries = 0
    while time.time() - t_wait < T(60) + 30 and task is None:
        for pod in pods:
            tries += 1
            ENV.signal(pod, "STOP")
            row = ENV.sql(f"SELECT id, attempts, token FROM tasks WHERE status = 'running' AND worker_id = '{pod}' "
                          "ORDER BY id LIMIT 1")
            if row:
                victim, task = pod, [int(x) for x in row.split("|")]
                break
            ENV.signal(pod, "CONT")
            time.sleep(0.3)
        if p.poll() is not None and task is None and tries > 6:
            p = taskgen(os.path.join(RES, "step5_tasks_extra.json"), rate=10, count=100, wait=False)
    if task is None:
        raise RuntimeError("не знайшла задачі в обробці за відведений час; запустіть step5 ще раз")
    tid, att0, tok0 = task
    print(f"   {victim} зупинено: задача {tid} (attempts {att0}, token {tok0}) у обробці; спроб {tries}")
    t_stop = time.time()
    time.sleep(2 * LEASE_S)
    mid = ENV.sql(f"SELECT status, attempts, token, coalesce(worker_id, '-') FROM tasks WHERE id = {tid}")
    ENV.signal(victim, "CONT")
    t_cont = time.time()
    p.communicate()
    wait_drain(T(300))
    time.sleep(2)
    final = ENV.sql(f"SELECT status, attempts, token, worker_id FROM tasks WHERE id = {tid}")
    n_results = int(ENV.sql(f"SELECT count(*) FROM task_results WHERE task_id = {tid}"))
    zero_rows = ENV.log_grep(victim, "0 rows updated")
    other = [x for x in pods if x != victim]
    requeued_lines = []
    for o in other:
        requeued_lines += [l for l in ENV.log_grep(o, "[requeued]") if f"задачу {tid} " in l]
    n, dup, _ = dup_stats()
    res = {"victim": victim, "task_id": tid, "before": {"attempts": att0, "token": tok0},
           "during_stop": mid, "after": final, "tries_to_catch_task": tries, "pid1_signal_probe": ENV.pid1_probe, "results_for_task": n_results,
           "stopped_s": t_cont - t_stop, "zero_rows_log_victim": zero_rows,
           "requeue_log_other": requeued_lines, "n_dup_results_all_tasks": dup, "tasks": n,
           "attempts_gt1": int(ENV.sql("SELECT count(*) FROM tasks WHERE attempts > 1")),
           "failed": int(ENV.sql("SELECT count(*) FROM tasks WHERE status = 'failed'"))}
    print(f"   після відновлення: {final}; результатів для задачі: {n_results}; рядків журналу про 0 оновлених: {len(zero_rows)}")
    save("step5", res)


def cmd_step6(a):
    """X(W) для W = 1, 2, R з непорожньою чергою: черга 400 задач (більше, щоб вікно останніх 30 с було сталим)."""
    ENV.sql_file(os.path.join(ROOT, "db", "lab4", "restore_results.sql"))
    out = {}
    x1 = None
    for w in a.workers:
        n_tasks = a.tasks
        if x1 is not None:
            n_tasks = max(n_tasks, int(math.ceil(T(40) * x1 * w)))   # щоб до спорожнення лишалось >= 30 с
        print(f"== W = {w}, задач у черзі {n_tasks}")
        default_env(1)                       # воркери з типовими змінними
        ENV.scale(0)                         # черга наповнюється, поки воркерів нема
        reset_tasks()
        taskgen(os.path.join(RES, f"step6_w{w}_tasks.json"), rate=200, count=n_tasks)
        sampler = Sampler()
        sampler.start()
        pods = ENV.scale(w)
        pods_before = snapshot_workers(pods)
        t_sc = time.time()
        drained = wait_drain(T(600))
        t_end = time.time()
        sampler.stop()
        after = snapshot_workers(pods)
        # вікно: останні 30 с до спорожнення за finished_at
        last = ENV.sql("SELECT extract(epoch FROM max(finished_at)) FROM tasks WHERE status = 'done'")
        t_last = float(last)
        win = min(T(30), max(1.0, t_last - t_sc))
        secs, counts = per_second_done(t_last - win, t_last)
        X = statistics.fmean(counts) if counts else None
        sd = statistics.pstdev(counts) if len(counts) > 1 else 0.0
        items, total = diff_hist(pods_before, after, "claim")
        rows = [r for r in sampler.rows if "xact" in r]
        in_win = [r for r in rows if t_last - win <= r["t"] <= t_last]
        tps = None
        if len(in_win) >= 2:
            tps = (in_win[-1]["xact"] - in_win[0]["xact"]) / (in_win[-1]["t"] - in_win[0]["t"])
        out[w] = {"W": w, "tasks": n_tasks, "drained": drained, "X": X, "std": sd, "window_s": win,
                  "per_second": counts, "claim_p99_s": quantile_from_buckets(items, total, 0.99),
                  "xact_per_s": tps, "db_cpu_cores": db_cpu_cores(),
                  "empty_claims": diff_counter(pods_before, after, "worker_claims_total", result="empty"),
                  "samples": rows, "t_last": t_last}
        print(f"   X({w}) = {X:.2f} задач/с, розкид {sd:.2f}, транзакцій/с {tps}")
        if w == 1:
            x1 = X
        save(f"step6_w{w}", out[w])
    save_state(X_by_W={str(k): v["X"] for k, v in out.items()}, **({f"X{R}": out[R]["X"]} if R in out else {}))


def db_cpu_cores():
    """Використання CPU пода бази (ядра) за останні 30 с із cAdvisor через Prometheus; None, якщо недоступно."""
    q = ('sum(rate(container_cpu_usage_seconds_total{namespace="studies",pod="db-0",container!="",'
         'container!="POD"}[30s]))')
    try:
        data = json.loads(http_get(f"{PROM}/api/v1/query?query=" + urllib.request.quote(q)))
        res = data["data"]["result"]
        return float(res[0]["value"][1]) if res else None
    except Exception:  # noqa: BLE001
        return None


def steady_run(label, rate, duration, workers=R, extra=None):
    """Генератор створює задачі з інтенсивністю rate протягом duration; усе знімається семплером."""
    pods = default_env(workers)
    reset_tasks()
    time.sleep(2)
    sampler = Sampler(with_gauge=True)
    sampler.start()
    t0 = time.time()
    s = taskgen(os.path.join(RES, f"{label}_tasks.json"), rate=rate, duration=duration)
    t_gen_end = time.time()
    q_end = int(ENV.sql("SELECT count(*) FROM tasks WHERE status = 'queued'"))
    drained = wait_drain(T(600))
    sampler.stop()
    return {"label": label, "rate": rate, "duration": duration, "t0": t0, "t_gen_end": t_gen_end,
            "queue_at_end": q_end, "drained": drained, "gen": s, "samples": sampler.rows, "gauge": sampler.gauge}


def window_stats(t_from, t_to):
    """За задачами, створеними у вікні: кількість і середнє started_at - created_at (з)."""
    r = ENV.sql("SELECT count(*), avg(extract(epoch FROM started_at - created_at)) FROM tasks "
                f"WHERE created_at >= to_timestamp({t_from}) AND created_at < to_timestamp({t_to}) AND started_at IS NOT NULL")
    n, w = r.split("|")
    return int(n), float(w) if w else None


def cmd_step7(a):
    ENV.sql_file(os.path.join(ROOT, "db", "lab4", "restore_results.sql"))
    xr = a.xr or load_state().get(f"X{R}")
    if not xr:
        sys.exit("потрібне X(R): виконайте step6 або вкажіть --xr")
    print(f"X(R) = {xr:.2f}")
    # сталий режим 0,7·X(R)
    rate = 0.7 * xr
    dur = T(180)
    res = steady_run("step7_steady", rate, dur)
    t_from, t_to = res["t0"] + T(60), res["t0"] + dur
    n, wq = window_stats(t_from, t_to)
    lam = n / (t_to - t_from)
    lq_vals = [g["tasks_queued"] for g in res["gauge"] if t_from <= g["t"] <= t_to and g["tasks_queued"] is not None]
    lq = statistics.fmean(lq_vals) if lq_vals else None
    res.update({"window": [t_from, t_to], "n_created_in_window": n, "lambda": lam, "W_q": wq, "Lq_measured": lq,
                "lambda_Wq": lam * wq if wq is not None else None,
                "diff_pct": (100 * (lq - lam * wq) / (lam * wq) if lq is not None and wq else None)})
    print(f"   λ = {lam:.2f}, W_q = {wq}, λ·W_q = {res['lambda_Wq']}, L_q = {lq}, розбіжність {res['diff_pct']} %")
    save("step7_steady", res)
    # перевантаження 1,3·X(R)
    over = steady_run("step7_overload", 1.3 * xr, T(120))
    first = ENV.sql("SELECT avg(extract(epoch FROM started_at - created_at)) FROM (SELECT * FROM tasks ORDER BY created_at, id "
                    "LIMIT 100) x WHERE started_at IS NOT NULL")
    last = ENV.sql("SELECT avg(extract(epoch FROM started_at - created_at)) FROM (SELECT * FROM tasks ORDER BY created_at DESC, id DESC "
                   "LIMIT 100) x WHERE started_at IS NOT NULL")
    over.update({"Wq_first100_s": float(first) if first else None, "Wq_last100_s": float(last) if last else None})
    print(f"   довжина черги на кінець генерації {over['queue_at_end']}, W_q перших 100 = {first}, останніх 100 = {last}")
    save("step7_overload", over)


def cmd_step8(a):
    ENV.sql_file(os.path.join(ROOT, "db", "lab4", "restore_results.sql"))
    xr = a.xr or load_state().get(f"X{R}")
    if not xr:
        sys.exit("потрібне X(R): виконайте step6 або вкажіть --xr")
    pods = default_env(R)
    reset_tasks()
    time.sleep(2)
    logs_dir = os.path.join(RES, "step8_logs")
    os.makedirs(logs_dir, exist_ok=True)
    followers = [ENV.follow_logs(p, os.path.join(logs_dir, p + ".log")) for p in pods]
    sampler = Sampler()
    sampler.start()
    dur = T(120)
    t0 = time.time()
    gen = taskgen(os.path.join(RES, "step8_tasks.json"), rate=0.7 * xr, duration=dur, wait=False)
    time.sleep(dur / 3)
    exit_codes = {}
    watch_path = os.path.join(logs_dir, "pods_watch.json")
    watcher, wf = ENV.watch_pods(watch_path)
    t_restart = time.time()
    ENV.rollout_restart()
    t_end = time.time() + T(90)
    while time.time() < t_end:
        try:
            st = ENV.pods()
        except Exception:  # noqa: BLE001
            st = []
        if len(st) == R and all(r for _, r, _ in st) and time.time() - t_restart > 10:
            break
        time.sleep(1)
    time.sleep(3)
    watcher.terminate()
    wf.close()
    exit_codes = parse_exit_codes(watch_path)
    gen.communicate()
    drained = wait_drain(T(300))
    sampler.stop()
    for pr, f in followers:
        time.sleep(0.5)
        pr.terminate()
        f.close()
    shutdown_lines, exit_log = [], 0
    for fn in os.listdir(logs_dir):
        txt = open(os.path.join(logs_dir, fn), encoding="utf-8", errors="replace").read()
        shutdown_lines += [l for l in txt.splitlines() if "[released_on_sigterm]" in l]
        exit_log += txt.count("[exit_code_0]")
    n, dup, _ = dup_stats()
    res = {"X_R": xr, "drained": drained, "t_restart": t_restart, "exit_codes_from_pod_status": exit_codes,
           "exit_code_0_in_logs": exit_log, "returned_to_queue_on_shutdown": shutdown_lines,
           "failed": int(ENV.sql("SELECT count(*) FROM tasks WHERE status = 'failed'")),
           "tasks": n, "n_dup_results": dup, "attempts_gt1": int(ENV.sql("SELECT count(*) FROM tasks WHERE attempts > 1")),
           "samples": sampler.rows}
    print(f"   коди завершення (за статусами подів): {exit_codes}; «вихід з кодом 0» у журналах: {exit_log}; "
          f"повернуто в чергу при зупинці: {len(shutdown_lines)}; failed {res['failed']}; дублів результатів {dup}; "
          f"attempts>1: {res['attempts_gt1']}")
    save("step8", res)


def fail_run(label, xr, lease, heartbeat, duration, fail_rate=0.15):
    """Прогін кроку 9: інтенсивність 0,7·X(R), FAIL_RATE = fail_rate, воркерів R, L і heartbeat задано.

    Порядок: (1) базовий стан без падінь і порожня черга, (2) воркерам задається FAIL_RATE, L, heartbeat (поди
    стартують на порожній черзі, тому стають готовими), (3) генерація duration секунд, (4) ЗНІМОК стану в кінці
    генерації: розподіл attempts за задачами, що вже завершені (done або failed), перезапуски контейнерів, рядки
    журналу «0 rows updated», (5) FAIL_RATE повертається в 0 і черга дочищається (інакше Kubernetes, збільшуючи паузу
    між перезапусками контейнерів, що падають, до 5 хв, лишив би чергу недоопрацьованою), (6) підсумкові
    перевірки (дублі результатів) після спорожнення."""
    default_env(R)                                     # без падінь, типові L і heartbeat
    reset_tasks()
    pods = ENV.set_env(R, FAIL_RATE=fail_rate, LEASE_SECONDS=lease, HEARTBEAT="true" if heartbeat else "false")
    time.sleep(2)
    sampler = Sampler(with_gauge=True)
    sampler.start()
    t0 = time.time()
    s = taskgen(os.path.join(RES, f"{label}_tasks.json"), rate=0.7 * xr, duration=duration)
    t_gen_end = time.time()

    # ---- знімок у кінці генерації (до зміни FAIL_RATE)
    pods_now = ENV.pods()
    restarts = {p: r for p, _, r in pods_now}
    zero_rows = sum(ENV.log_grep_count(p, "0 rows updated") for p, _, _ in pods_now)
    dist = ENV.sql("SELECT attempts, count(*) FROM tasks WHERE status IN ('done', 'failed') GROUP BY 1 ORDER BY 1")
    snap = ENV.sql("SELECT count(*) FILTER (WHERE status = 'queued'), count(*) FILTER (WHERE status = 'running'), "
                   "count(*) FILTER (WHERE status = 'done'), count(*) FILTER (WHERE status = 'failed'), "
                   "count(*) FILTER (WHERE status IN ('done', 'failed') AND attempts > 1), count(*) FROM tasks")
    q_, r_, d_, f_, gt1, total = (int(x) for x in snap.split("|"))
    terminal = d_ + f_
    attempts_dist = {int(l.split("|")[0]): int(l.split("|")[1]) for l in dist.splitlines()}

    # ---- дочищення черги без падінь
    ENV.set_env(R, FAIL_RATE="0", LEASE_SECONDS=LEASE_S, HEARTBEAT="true")
    drained = wait_drain(T(900))
    t_end = time.time()
    sampler.stop()
    n, dup, _ = dup_stats()
    secs, counts = per_second_done(t0, t_end)
    res = {"label": label, "fail_rate": fail_rate, "lease_s": lease, "heartbeat": heartbeat, "rate": 0.7 * xr,
           "duration": duration, "t0": t0, "t_gen_end": t_gen_end, "t_end": t_end, "drained": drained,
           "restarts": restarts, "restarts_total": sum(restarts.values()),
           "snapshot_at_generation_end": {"queued": q_, "running": r_, "done": d_, "failed": f_, "tasks": total,
                                          "terminal": terminal, "attempts_gt1_among_terminal": gt1},
           "attempts_distribution_terminal": attempts_dist,
           "tasks": n, "failed_final": int(ENV.sql("SELECT count(*) FROM tasks WHERE status = 'failed'")),
           "n_dup_results": dup, "zero_rows_log_lines": zero_rows, "unreadable_log_pods": len(set(getattr(ENV, "unreadable", []))),
           "done_in_window": d_, "X_run": d_ / duration,
           "done_secs": secs, "done_per_s": counts, "samples": sampler.rows, "gauge": sampler.gauge}
    share = gt1 / terminal if terminal else float("nan")
    share_failed = f_ / terminal if terminal else float("nan")
    print(f"   {label}: у кінці генерації завершено {terminal} із {total} задач, X = {d_ / duration:.2f} задач/с; "
          f"перезапусків контейнерів {res['restarts_total']}; attempts>1: {gt1}/{terminal} ({share:.3f}); "
          f"failed {f_}/{terminal} ({share_failed:.3f}); дублів результатів {dup}; рядків «0 rows updated» {zero_rows}")
    save(label, res)
    return res


def cmd_step9(a):
    ENV.sql_file(os.path.join(ROOT, "db", "lab4", "restore_results.sql"))
    st = load_state()
    xr = a.xr or st.get(f"X{R}")
    if not xr:
        sys.exit("потрібне X(R): виконайте step6 або вкажіть --xr")
    t_cpu = (a.t_cpu_ms or st.get("t_cpu_ms"))
    if not t_cpu:
        sys.exit("потрібне t_cpu: виконайте step2 або вкажіть --t-cpu-ms")
    dur = T(300)
    only = set(a.only) if a.only else {"fail015", "f5_nohb", "f5_hb"}
    lease = t_cpu / 1000 / 2
    # A. FAIL_RATE = 0,15, L варіанта з heartbeat (без Ф5): порівняння X і розподілу attempts з X(R) без падінь
    if "fail015" in only:
        fail_run("step9_fail015", xr, LEASE_S, True, dur)
    # B, C. Сценарій Ф5: L = t_cpu / 2, спершу без heartbeat (B), далі з heartbeat (C); FAIL_RATE = 0,15 в обох
    if "f5_nohb" in only:
        fail_run("step9_f5_nohb", xr, lease, False, dur)
    if "f5_hb" in only:
        fail_run("step9_f5_hb", xr, lease, True, dur)
    default_env(R)   # повертаємо типові значення


def cmd_d2(a):
    ENV.sql_file(os.path.join(ROOT, "db", "lab4", "restore_results.sql"))
    default_env(R)
    reset_tasks()
    s = taskgen(os.path.join(RES, "d2_tasks.json"), rate=5, count=100, delay=(10, 60))
    ok = wait_drain(T(180))
    q = ENV.sql("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY d), percentile_cont(0.99) WITHIN GROUP (ORDER BY d), "
                "min(d), max(d), count(*) FROM (SELECT extract(epoch FROM started_at - run_after) AS d FROM tasks "
                "WHERE started_at IS NOT NULL) x")
    p50, p99, mn, mx, n = q.split("|")
    early = int(ENV.sql("SELECT count(*) FROM tasks WHERE started_at < run_after"))
    delays = ENV.sql("SELECT min(extract(epoch FROM run_after - created_at)), max(extract(epoch FROM run_after - created_at)) FROM tasks")
    res = {"drained": ok, "n": int(n), "p50_s": float(p50), "p99_s": float(p99), "min_s": float(mn), "max_s": float(mx),
           "started_before_run_after": early, "delay_range_s": delays}
    print(f"   started_at - run_after: p50 {float(p50) * 1000:.1f} мс, p99 {float(p99) * 1000:.1f} мс, "
          f"раніше run_after: {early}")
    save("d2", res)


STEPS = {"prep": cmd_prep, "step2": cmd_step2, "step3": cmd_step3, "step4": cmd_step4, "step5": cmd_step5,
         "step6": cmd_step6, "step7": cmd_step7, "step8": cmd_step8, "step9": cmd_step9, "d2": cmd_d2}


def main():
    global TIME_SCALE, ENV
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=list(STEPS))
    ap.add_argument("--time-scale", type=float, default=1.0)
    ap.add_argument("--workers", type=int, nargs="+", default=[1, 2, R], help="step6: значення W")
    ap.add_argument("--tasks", type=int, default=400, help="step6: задач у черзі перед прогоном")
    ap.add_argument("--xr", type=float, default=None, help="X(R), якщо не збережено в state.json")
    ap.add_argument("--t-cpu-ms", type=float, default=None)
    ap.add_argument("--only", nargs="+", choices=["fail015", "f5_nohb", "f5_hb"], default=None, help="step9: які прогони")
    a = ap.parse_args()
    TIME_SCALE = a.time_scale
    os.makedirs(RES, exist_ok=True)
    STEPS[a.step](a)


if __name__ == "__main__":
    main()
