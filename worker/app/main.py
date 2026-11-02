"""Воркер черги задач (ЛР4). Один процес, одна задача за раз; паралелізм пулу задає кількість подів.

Задача складається з трьох кроків: fetch (HTTP до web), process (обробка зрізу), store (запис результату).
Heartbeat працює в окремому потоці, тому продовжує lease і під час обробки, яка тримає основний потік.
"""
import http.client
import json
import logging
import os
import random
import signal
import sys
import threading
import time
import urllib.parse

import psycopg
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .config import load_settings
from .processing import process_slice
from .queue_db import Heartbeat, Queue

settings = load_settings()
s = settings
logging.basicConfig(level=s.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s %(message)s",
                    handlers=[logging.StreamHandler(), logging.FileHandler(s.log_file, encoding="utf-8")])
log = logging.getLogger("worker")

TASKS = Counter("worker_tasks_total", "Задачі за результатом", ["result"])           # done, failed, lost
CLAIMS = Counter("worker_claims_total", "Спроби захоплення за результатом", ["result"])  # task, empty
DURATION = Histogram("worker_task_duration_seconds", "Тривалість кроків задачі, с", ["step"],
                     buckets=s.buckets_s)                                            # claim, fetch, process, store
IN_FLIGHT = Gauge("worker_tasks_in_flight", "Задачі в обробці")
for _r in ("done", "failed", "lost"):
    TASKS.labels(_r)
for _r in ("task", "empty"):
    CLAIMS.labels(_r)
for _st in ("claim", "fetch", "process", "store"):
    DURATION.labels(_st)

stopping = False
current = {"task": None}   # (id, token) задачі в обробці, потрібно для повернення в чергу після SIGTERM


class Fetcher:
    """Постійне HTTP-з'єднання до web; після помилки відкривається нове."""

    def __init__(self):
        u = urllib.parse.urlparse(s.web_url)
        self.host, self.port = u.hostname, u.port or 80
        self.conn = None

    def get_record(self, record_id: int) -> dict:
        for attempt in (1, 2):
            try:
                if self.conn is None:
                    self.conn = http.client.HTTPConnection(self.host, self.port, timeout=s.http_timeout_s)
                self.conn.request("GET", f"/studies/{record_id}")
                resp = self.conn.getresponse()
                body = resp.read()
                if resp.status != 200:
                    raise RuntimeError(f"GET /studies/{record_id} -> {resp.status}")
                return json.loads(body)
            except (http.client.HTTPException, OSError):
                self.conn = None   # мертве з'єднання: одна повторна спроба з новим
                if attempt == 2:
                    raise


def on_sigterm(signum, frame):
    """Припиняє захоплення нових задач. Якщо поточна задача не завершиться за GRACE_SECONDS,
    таймер повертає її в чергу командою з умовою за токеном і виходить."""
    global stopping
    if stopping:
        return
    stopping = True
    log.info("SIGTERM: нові задачі не захоплюю, завершую поточну (до %s с)", s.grace_s)
    t = threading.Timer(s.grace_s, force_release)
    t.daemon = True
    t.start()


def force_release():
    task = current["task"]
    if task is not None:
        tid, token = task
        try:
            from .queue_db import connect, rows, SQL_RELEASE
            c = connect(s)
            ok = bool(rows(c, SQL_RELEASE, (tid, token)))
            log.warning("[released_on_sigterm] час очікування вичерпано: задачу %s повернуто в чергу: %s", tid, ok)
        except Exception as exc:  # noqa: BLE001
            log.error("не вдалося повернути задачу %s в чергу: %r", tid, exc)
    os._exit(0)


def run_task(q: Queue, fetcher: Fetcher, hb, task) -> None:
    tid, record_id, seed, attempts, token = task
    current["task"] = (tid, token)
    IN_FLIGHT.inc()
    try:
        if hb is not None:
            hb.set_task(tid, token)
        t = time.perf_counter()
        fetcher.get_record(record_id)
        DURATION.labels("fetch").observe(time.perf_counter() - t)

        t = time.perf_counter()
        result = process_slice(seed, s.slice_size, s.q_lo, s.q_hi)
        DURATION.labels("process").observe(time.perf_counter() - t)

        # вбудоване падіння: після обробки і до запису; без обробки сигналів і закриття з'єднань
        if s.fail_rate > 0 and random.random() < s.fail_rate:
            log.error("FAIL_RATE: процес падає, задача %s токен %s лишається в running", tid, token)
            os._exit(1)

        t = time.perf_counter()
        ok = q.finish(tid, token, result)
        DURATION.labels("store").observe(time.perf_counter() - t)
        TASKS.labels("done" if ok else "lost").inc()
        if ok:
            log.info("[task_done] задача %s виконано (спроба %s, токен %s)", tid, attempts, token)
    except (psycopg.Error, OSError, RuntimeError, ValueError) as exc:
        TASKS.labels("failed").inc()
        log.error("задача %s: помилка %r; lease спливе, і задачу повернуть у чергу", tid, exc)
        q.reset()
    finally:
        if hb is not None:
            hb.clear()
        IN_FLIGHT.dec()
        current["task"] = None


def main() -> int:
    sys.setswitchinterval(0.001)   # потік heartbeat швидше отримує GIL, поки основний потік рахує
    signal.signal(signal.SIGTERM, on_sigterm)
    start_http_server(s.metrics_port)
    log.info("воркер %s: режим %s, T_poll=%.0f мс, L=%s с, heartbeat=%s, FAIL_RATE=%s", s.worker_id,
             s.claim_mode, s.poll_s * 1000, s.lease_s, s.heartbeat, s.fail_rate)
    q = Queue(s)
    fetcher = Fetcher()
    hb = None
    if s.claim_mode != "naive" and s.heartbeat:
        hb = Heartbeat(s)
        hb.start()
    reap_every = min(1.0, s.lease_s / 2)   # повернення прострочених задач перед захопленням, не частіше за цей інтервал
    last_reap = 0.0
    while not stopping:
        try:
            if s.claim_mode != "naive" and time.monotonic() - last_reap >= reap_every:
                last_reap = time.monotonic()
                q.requeue_expired()
            t = time.perf_counter()
            task = q.claim()
            DURATION.labels("claim").observe(time.perf_counter() - t)
        except psycopg.Error as exc:
            log.warning("база недоступна: %r; повторю через 1 с", exc)
            q.reset()
            time.sleep(1)
            continue
        if task is None:
            CLAIMS.labels("empty").inc()
            time.sleep(s.poll_s)
            continue
        CLAIMS.labels("task").inc()
        run_task(q, fetcher, hb, task)
    log.info("[exit_code_0] зупинено за SIGTERM, вихід з кодом 0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
