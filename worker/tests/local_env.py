#!/usr/bin/env python3
"""Локальний запуск scripts/lab4.py без кластера: воркери це локальні процеси, база справжній PostgreSQL,
сервіс web замінює заглушка з тими самими ендпоінтами черги. Призначений для перевірки логіки скриптів.
  TEST_PG_PORT=54329 python3 worker/tests/local_env.py <крок> [параметри lab4.py]"""
import http.server, json, os, random, shlex, signal, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
WORKER = os.path.dirname(HERE)
ROOT = os.path.dirname(WORKER)
sys.path.insert(0, f"{ROOT}/scripts")
sys.path.insert(0, f"{HERE}/psycopg_shim")
import psycopg  # noqa: E402  (підміна)
import lab4  # noqa: E402

PORT = os.environ.get("TEST_PG_PORT", "54329")
PSQL = ["psql", "-X", "-At", "-F", "|", "-h", "127.0.0.1", "-p", PORT, "-U", "studies_app", "-d", "studies"]
WEB_PORT = 18080


class FakeWeb(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    conn = psycopg.connect("127.0.0.1", PORT, "studies", "studies_app")
    lock = threading.Lock()

    def _send(self, code, body, ctype="application/json"):
        b = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(b)))
        self.end_headers(); self.wfile.write(b)

    def do_GET(self):
        if self.path.startswith("/metrics"):
            with self.lock:
                q, r = self.conn.execute("SELECT count(*) FILTER (WHERE status='queued'), count(*) FILTER (WHERE status='running') FROM tasks").fetchall()[0]
            self._send(200, f"tasks_queued {q}\ntasks_running {r}\n".encode(), "text/plain")
        else:
            self._send(200, {"id": 1})

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0)); self.rfile.read(n)
        import re
        m = re.match(r"/studies/(\d+)/tasks(?:\?delay_s=([\d.]+))?", self.path)
        delay = float(m.group(2) or 0)
        with self.lock:
            row = self.conn.execute("INSERT INTO tasks (record_id, seed, run_after) SELECT %s, %s, now() + make_interval(secs => %s) RETURNING id",
                                    (int(m.group(1)), random.randint(1, 2**31 - 1), delay)).fetchall()
        self._send(202, {"id": int(row[0][0])})

    def log_message(self, *a):
        pass


class LocalEnv:
    def __init__(self):
        self.procs = {}      # імʼя -> {"p": Popen, "env": dict, "restarts": int, "stop": bool}
        self.env = {}        # поточні змінні воркерів
        self.seq = 0
        self.lock = threading.Lock()
        threading.Thread(target=self._supervise, daemon=True).start()

    # --- база
    def sql(self, q):
        r = subprocess.run(PSQL + ["-c", q], capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError(r.stderr)
        return r.stdout.strip()

    def sql_file(self, path):
        r = subprocess.run(PSQL + ["-q", "-v", "ON_ERROR_STOP=1", "-f", path], capture_output=True, text=True)
        if r.returncode:
            raise RuntimeError(r.stderr)

    # --- воркери
    def _env(self, name, idx):
        e = dict(os.environ)
        base = dict(DB_HOST="127.0.0.1", DB_PORT=PORT, DB_NAME="studies", DB_USER="studies_app", DB_PASSWORD="x",
                    WEB_URL=f"http://127.0.0.1:{WEB_PORT}", WORKER_ID=name, CLAIM_MODE="skip_locked", POLL_INTERVAL_MS="10",
                    LEASE_SECONDS="5", HEARTBEAT="true", FAIL_RATE="0", SLICE_SIZE="256", Q_LO="0.05", Q_HI="0.95",
                    HISTOGRAM_BUCKETS_MS="1,2,4,8,16,32,64,128,256,512,1024", METRICS_PORT=str(19200 + idx), GRACE_SECONDS="25",
                    HTTP_TIMEOUT_SECONDS="5", LOG_LEVEL="info", LOG_FILE=f"/tmp/lab4w_{name}.log",
                    PYTHONPATH=f"{HERE}/psycopg_shim:{HERE}/prom_shim:{WORKER}")
        base.update(self.env)
        e.update({k: str(v) for k, v in base.items()})
        return e

    def _spawn(self, name, idx):
        e = self._env(name, idx)
        p = subprocess.Popen([sys.executable, "-m", "app"], cwd=WORKER, env=e, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return {"p": p, "idx": idx, "restarts": self.procs.get(name, {}).get("restarts", 0), "stop": False, "port": 19200 + idx}

    def _supervise(self):
        while True:
            time.sleep(0.3)
            with self.lock:
                for name, d in list(self.procs.items()):
                    rc = d["p"].poll()
                    if rc is not None and rc != 0 and not d["stop"]:
                        nd = self._spawn(name, d["idx"]); nd["restarts"] = d["restarts"] + 1
                        self.procs[name] = nd

    def _kill_all(self):
        for d in self.procs.values():
            d["stop"] = True
            d["p"].terminate()
        for d in self.procs.values():
            try:
                d["p"].wait(timeout=30)
            except subprocess.TimeoutExpired:
                d["p"].kill()
        self.procs = {}

    def _start(self, n):
        with self.lock:
            self._kill_all()
            for i in range(n):
                self.seq += 1
                name = f"worker-{self.seq}"
                try:
                    os.remove(f"/tmp/lab4w_{name}.log")
                except FileNotFoundError:
                    pass
                self.procs[name] = self._spawn(name, i)
        return self.wait_workers(n)

    def wait_workers(self, n, seconds=60):
        t0 = time.time()
        while time.time() - t0 < seconds:
            names = [k for k, d in self.procs.items() if d["p"].poll() is None]
            if len(names) == n:
                ok = 0
                for k in names:
                    try:
                        lab4.http_get(f"http://127.0.0.1:{self.procs[k]['port']}/metrics", 1); ok += 1
                    except Exception:
                        pass
                if ok == n:
                    return names
            time.sleep(0.3)
        raise RuntimeError("воркери не стартували")

    def pods(self):
        return [(k, d["p"].poll() is None, d["restarts"]) for k, d in self.procs.items()]

    def all_pods_json(self):
        return {"items": []}

    def scale(self, n):
        cur = len(self.procs)
        if n == cur:
            return self.wait_workers(n)
        return self._start(n)

    def set_env(self, n, **kv):
        self.env.update(kv)
        return self._start(n)

    def rollout_restart(self):
        n = len(self.procs)
        threading.Thread(target=self._start, args=(n,), daemon=True).start()

    def scrape(self, pod):
        return lab4.parse_metrics(lab4.http_get(f"http://127.0.0.1:{self.procs[pod]['port']}/metrics"))

    def log_grep(self, pod, pattern):
        try:
            return [l for l in open(f"/tmp/lab4w_{pod}.log", encoding="utf-8") if pattern in l]
        except FileNotFoundError:
            return []

    def log_grep_count(self, pod, pattern):
        return len(self.log_grep(pod, pattern))

    pid1_probe = {"command": "локальна перевірка", "works": True}

    def signal(self, pod, sig):
        os.kill(self.procs[pod]["p"].pid, getattr(signal, "SIG" + sig))

    def watch_pods(self, path):
        class P:
            def terminate(s_): pass
        class F:
            def close(s_): open(path, "w").write("")
        return P(), F()

    def follow_logs(self, pod, path):
        class P:
            def terminate(s_):
                try:
                    open(path, "w").write(open(f"/tmp/lab4w_{pod}.log", encoding="utf-8").read())
                except FileNotFoundError:
                    pass
        class F:
            def close(s_): pass
        return P(), F()


if __name__ == "__main__":
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", WEB_PORT), FakeWeb)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    lab4.ENV = LocalEnv()
    lab4.BASE = f"http://127.0.0.1:{WEB_PORT}"
    lab4.PROM = "http://127.0.0.1:1"
    lab4.RES = os.environ.get("TEST_RES", "/tmp/lab4res")
    os.makedirs(lab4.RES, exist_ok=True)
    sys.argv = ["lab4.py"] + sys.argv[1:]
    try:
        lab4.main()
    finally:
        lab4.ENV._kill_all()
