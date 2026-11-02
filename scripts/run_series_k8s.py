#!/usr/bin/env python3
"""ЛР3, крок 7: серія ступінчастих прогонів без автомасштабування на N = 1..R реплік.

Для кожного N:
  - kubectl scale deployment/web --replicas=N; чекає, поки готові рівно N подів, Prometheus бачить
    N цілей UP, а через NodePort відповідають усі N подів (за X-Pod);
  - запускає loadgen/stepload.py (40 постійних зʼєднань, закриття після 100 запитів, сходинка 30 с);
  - вивантажує сирі вибірки Prometheus за час прогону у results/<тег>prom_nN.json
    і записує, на яких вузлах стояли поди.

Запуск з кореня проєкту (Git Bash):
  python scripts/run_series_k8s.py                  # усі N = 1..5
  python scripts/run_series_k8s.py --ns 1 2         # лише частина
  python scripts/run_series_k8s.py --tag rep2_      # повтор серії (інші імена файлів)
  python scripts/run_series_k8s.py --step-rate 50   # приріст сходинки (за замовчуванням 50)

Необовʼязково: `pip install psutil` — тоді знімається завантаження CPU і памʼяті хоста
(це лаптоп: хост може бути вузьким місцем, як у ЛР2).
"""
import argparse
import json
import os
import shlex
import subprocess
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(__file__))
import prom_export  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RES = os.path.join(ROOT, "results")
PY = sys.executable
CLUSTER = os.environ.get("CLUSTER", "studies")
CTX = f"k3d-{CLUSTER}"
R = 5   # найбільша кількість реплік за варіантом: 3 + (5 mod 3) = 5


def kubectl_cmd():
    """Запасний режим: KUBECTL_CMD="docker exec k3d-studies-server-0 kubectl" виконує kubectl усередині вузла
    k3d, повз проксі портів Docker Desktop (коли kubectl з Windows не підключається)."""
    custom = os.environ.get("KUBECTL_CMD")
    return shlex.split(custom) if custom else ["kubectl", "--context", CTX]


def kubectl(*args, check=True):
    r = subprocess.run([*kubectl_cmd(), *args], cwd=ROOT, text=True, capture_output=True)
    if check and r.returncode != 0:
        sys.exit(f"kubectl {' '.join(args)}: {r.stderr.strip()}")
    return r.stdout


def http_ok(url, timeout=1.5):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status == 200
    except Exception:  # noqa: BLE001
        return False


def wait(cond, what, seconds=180):
    for _ in range(seconds):
        if cond():
            return
        time.sleep(1)
    sys.exit(f"не дочекались: {what}")


def web_pods():
    """Поди web без тих, що вже видаляються: [(імʼя, готовий, вузол)]."""
    data = json.loads(kubectl("-n", "studies", "get", "pods", "-l", "app=web", "-o", "json"))
    out = []
    for p in data["items"]:
        if p["metadata"].get("deletionTimestamp"):
            out.append((p["metadata"]["name"], False, p["spec"].get("nodeName"), True))
            continue
        ready = any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))
        out.append((p["metadata"]["name"], ready, p["spec"].get("nodeName"), False))
    return out


def prom_targets_up(prom):
    try:
        data = prom_export.api(prom, "/api/v1/targets", {"state": "active"})
    except Exception:  # noqa: BLE001
        return 0
    return sum(1 for t in data["activeTargets"] if t["labels"].get("job") == "web" and t["health"] == "up")


def scale_to(n, base, prom):
    kubectl("-n", "studies", "scale", "deployment/web", f"--replicas={n}")

    def settled():
        pods = web_pods()
        return len(pods) == n and all(ready and not dying for _, ready, _, dying in pods)

    wait(settled, f"рівно {n} готових подів web", 240)
    wait(lambda: http_ok(f"{base}/healthz"), "healthz = 200")
    wait(lambda: prom_targets_up(prom) == n, f"{n} цілей web у стані UP у Prometheus", 120)
    # Через NodePort мають відповідати всі n подів. Кожен запит urllib іде новим зʼєднанням,
    # а Service обирає под під час встановлення зʼєднання.
    seen = set()
    for _ in range(80 * n):
        try:
            with urllib.request.urlopen(f"{base}/livez", timeout=2) as r:
                seen.add(r.headers.get("X-Pod"))
        except Exception:  # noqa: BLE001
            pass
        if len(seen) == n:
            break
    if len(seen) != n:
        sys.exit(f"через NodePort відповідають {len(seen)} подів із {n}: {seen}")
    print(f"  готово: {n} под(ів) web, Prometheus бачить {n} цілей UP, X-Pod: {sorted(seen)}", flush=True)
    time.sleep(15)   # метрики кожного пода мають встигнути стабілізуватись
    return {name: node for name, _, node, _ in web_pods()}


def ensure_ids(base):
    ids_path = os.path.join(ROOT, "loadgen", "ids.json")
    if os.path.exists(ids_path):
        with open(ids_path) as f:
            first = json.load(f)[0]
        if http_ok(f"{base}/studies/{first}"):
            return
    subprocess.run([PY, "loadgen/seed.py", "--url", base, "--count", "100", "--out", ids_path], cwd=ROOT, check=True)


class HostSampler(threading.Thread):
    """Раз на ~5 с знімає завантаження CPU і памʼяті хоста (потрібен psutil)."""

    def __init__(self, path):
        super().__init__(daemon=True)
        self.path, self.stop_flag = path, threading.Event()
        try:
            import psutil  # noqa: PLC0415
            self.psutil = psutil
            psutil.cpu_percent(None)
        except ImportError:
            self.psutil = None

    def run(self):
        if not self.psutil:
            return
        with open(self.path, "w") as f:
            while not self.stop_flag.is_set():
                t = time.time()
                rec = {"ts": t, "host_cpu_percent": self.psutil.cpu_percent(None),
                       "host_mem_percent": self.psutil.virtual_memory().percent}
                f.write(json.dumps(rec) + "\n")
                f.flush()
                self.stop_flag.wait(max(0.0, 5 - (time.time() - t)))


def stepload(a, name, extra):
    cmd = [PY, "-u", "loadgen/stepload.py", "--url", a.base, "--name", name, "--procs", str(a.procs),
           "--conns", str(8 * R), *extra]
    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.PIPE, text=True)
    t0 = None
    for line in proc.stdout:
        print("  ", line.rstrip(), flush=True)
        if line.startswith("T0_UNIX="):
            t0 = float(line.split("=")[1])
    proc.wait()
    if proc.returncode != 0 or t0 is None:
        sys.exit(f"stepload.py завершився з помилкою ({proc.returncode})")
    with open(os.path.join(RES, f"{name}.json")) as f:
        return t0, json.load(f)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ns", type=int, nargs="+", default=list(range(1, R + 1)))
    ap.add_argument("--base", default="http://localhost:8080")
    ap.add_argument("--prom", default="http://localhost:9090")
    ap.add_argument("--procs", type=int, default=max(1, (os.cpu_count() or 2) // 2),
                    help="процесів генератора; однакове в усіх прогонах роботи")
    ap.add_argument("--step-rate", type=int, default=50, help="початкова інтенсивність і приріст сходинки, запитів/с")
    ap.add_argument("--max-rate", type=int, default=4000, help="запобіжник: найбільша задана інтенсивність")
    ap.add_argument("--tag", default="", help="префікс імен файлів, напр. rep2_")
    a = ap.parse_args()

    os.makedirs(RES, exist_ok=True)
    if subprocess.run([*kubectl_cmd(), "get", "nodes"], capture_output=True).returncode != 0:
        sys.exit("Кластер недоступний: спершу scripts/cluster.sh up")
    manifest_path = os.path.join(RES, f"{a.tag}manifest.json")
    manifest = {"runs": {}}
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)
    dep = json.loads(kubectl("-n", "studies", "get", "deployment", "web", "-o", "json"))
    c = dep["spec"]["template"]["spec"]["containers"][0]
    cm = json.loads(kubectl("-n", "studies", "get", "configmap", "web-config", "-o", "json"))["data"]
    manifest.update({
        "procs": a.procs, "step_rate": a.step_rate, "step_seconds": 30, "transient_s": 10,
        "conns": 8 * R, "reqs_per_conn": 100, "R": R,
        "resources": c["resources"], "db_pool_size": cm["DB_POOL_SIZE"], "startup_delay_s": cm["STARTUP_DELAY_SECONDS"],
        "nodes": kubectl("get", "nodes", "-o", "wide").strip().splitlines(),
    })

    def save():
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

    for n in a.ns:
        print(f"=== Крок 7: {n} под(ів) web ===", flush=True)
        placement = scale_to(n, a.base, a.prom)
        ensure_ids(a.base)
        name = f"{a.tag}step_n{n}"
        sampler = HostSampler(os.path.join(RES, f"{name}_host.jsonl"))
        sampler.start()
        t_begin = time.time()
        try:
            t0, res = stepload(a, name, ["--step-rate", str(a.step_rate), "--step-seconds", "30",
                                          "--transient", "10", "--max-rate", str(a.max_rate)])
        finally:
            sampler.stop_flag.set()
            sampler.join(timeout=10)
        time.sleep(10)   # дочекатись останніх скрейпів Prometheus (інтервал 5 с; cAdvisor оновлюється рідше)
        samples = None
        for attempt in range(12):   # після перевантаження Prometheus може відповісти не одразу
            try:
                samples = prom_export.export(a.prom, t_begin - 5, time.time(),
                                             os.path.join(RES, f"{a.tag}prom_n{n}.json"), extra={"placement": placement})
                break
            except Exception as exc:  # noqa: BLE001
                print(f"  Prometheus не відповів ({type(exc).__name__}), повтор {attempt + 1}/12 через 10 с", flush=True)
                time.sleep(10)
        if samples is None:
            print(f"  УВАГА: вибірки Prometheus для N={n} не вивантажено; пізніше: "
                  f"python scripts/prom_export.py --run {name} --out results/{a.tag}prom_n{n}.json", flush=True)
        unreached = res["stop_reason"] == "max_rate"
        manifest["runs"][str(n)] = {
            "name": name, "t0_unix": t0, "X_rps": res["X_rps"], "X_std_rps": res["X_std_rps"], "X_step": res["X_step"],
            "stop_reason": res["stop_reason"], "unreached": unreached, "prom_samples": samples, "placement": placement,
        }
        save()
        flag = "  <- ТОЧКА НЕ ДОСЯГНУТА (генератор дійшов до --max-rate)" if unreached else ""
        print(f"  N={n}: X = {res['X_rps']} ± {res['X_std_rps']} rps (сходинка {res['X_step']}){flag}", flush=True)
        time.sleep(20)   # пауза між прогонами: черги і зʼєднання мають спорожніти

    print("\nПідсумок серії (X(N), rps):")
    for n, r in sorted(manifest["runs"].items(), key=lambda kv: int(kv[0])):
        print(f"  N={n}: {r['X_rps']} ± {r['X_std_rps']}  (сходинка {r['X_step']}, зупинка: {r['stop_reason']})")
    print("\nДалі: надішліть мені теку results/ (zip) або вивід цього підсумку.")


if __name__ == "__main__":
    main()
