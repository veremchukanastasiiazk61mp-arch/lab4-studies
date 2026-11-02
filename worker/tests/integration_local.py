#!/usr/bin/env python3
"""Локальна перевірка без кластера: справжній PostgreSQL + підміна psycopg через psql + заглушка web.
Запуск: TEST_PG_PORT=54329 python3 tests/integration_local.py  (з каталогу worker)"""
import http.server, json, os, subprocess, sys, threading, time

PORT = os.environ.get("TEST_PG_PORT", "54329")
HERE = os.path.dirname(os.path.abspath(__file__))
WORKER = os.path.dirname(HERE)
PSQL = ["psql", "-X", "-q", "-A", "-t", "-h", "127.0.0.1", "-p", PORT, "-U", "studies_app", "-d", "studies"]


def sql(q, check=True):
    r = subprocess.run(PSQL + ["-c", q], capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError(r.stderr)
    return r.stdout.strip()


class Web(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        body = json.dumps({"id": 1}).encode()
        self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)

    def log_message(self, *a):
        pass


def start_web(port=18080):
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", port), Web)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def worker_env(wid, mode, poll_ms, lease=5, fail=0, hb="true", mport=19000, grace=25):
    e = dict(os.environ)
    e.update(PYTHONPATH=f"{HERE}/psycopg_shim:{HERE}/prom_shim:{WORKER}", DB_HOST="127.0.0.1", DB_PORT=PORT, DB_NAME="studies",
             DB_USER="studies_app", DB_PASSWORD="x", WEB_URL="http://127.0.0.1:18080", WORKER_ID=wid,
             LEASE_SECONDS=str(lease), POLL_INTERVAL_MS=str(poll_ms), FAIL_RATE=str(fail), CLAIM_MODE=mode,
             HEARTBEAT=hb, SLICE_SIZE="256", Q_LO="0.05", Q_HI="0.95", HISTOGRAM_BUCKETS_MS="1,2,4,8,16,32,64,128,256,512,1024",
             METRICS_PORT=str(mport), GRACE_SECONDS=str(grace), HTTP_TIMEOUT_SECONDS="5", LOG_LEVEL="info", LOG_FILE=f"/tmp/worker_{wid}.log")
    return e


def spawn(wid, mode, poll_ms, **kw):
    return subprocess.Popen([sys.executable, "-m", "app"], cwd=WORKER, env=worker_env(wid, mode, poll_ms, **kw),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def reset(n_tasks, naive=False):
    sql("TRUNCATE tasks RESTART IDENTITY CASCADE", check=True)
    sql("INSERT INTO studies (patient_code, modality, slice_count, slice_thickness_mm, device, study_date) "
        "SELECT 'T'||g, 'CT', 10, 1, 'dev', now()::date FROM generate_series(1,5) g ON CONFLICT DO NOTHING", check=False)
    subprocess.run(PSQL + ["-f", f"{WORKER}/../db/lab4/{'naive_results' if naive else 'restore_results'}.sql"],
                   capture_output=True, text=True, check=True)
    sql(f"INSERT INTO tasks (record_id, seed) SELECT (SELECT min(id) FROM studies), g FROM generate_series(1,{n_tasks}) g")


def wait_empty(timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if sql("SELECT count(*) FROM tasks WHERE status IN ('queued','running')") == "0":
            return True
        time.sleep(0.3)
    return False


def dup_stats():
    return int(sql("SELECT count(*) FROM (SELECT task_id FROM task_results GROUP BY 1 HAVING count(*)>1) x"))


def series(mode, poll_ms, n_tasks=100, workers=5, naive=False):
    reset(n_tasks, naive=naive)
    procs = [spawn(f"w{i}", mode, poll_ms, mport=19000 + i) for i in range(workers)]
    ok = wait_empty()
    for p in procs:
        p.terminate()
    outs = [p.communicate(timeout=10)[0] for p in procs]
    codes = [p.returncode for p in procs]
    return ok, codes, outs


if __name__ == "__main__":
    start_web()
    for mode, poll, naive in (("naive", 10, True), ("naive", 1, True), ("skip_locked", 1, False), ("for_update", 1, False)):
        t = time.time()
        ok, codes, outs = series(mode, poll, naive=naive)
        n_res = sql("SELECT count(*) FROM task_results")
        print(f"{mode:12s} poll={poll:>2}мс  завершено={ok} час={time.time()-t:5.1f}с  результатів={n_res}/100 "
              f"задач із >1 результатом={dup_stats()} коди={set(codes)}")
