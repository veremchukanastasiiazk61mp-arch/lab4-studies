#!/usr/bin/env python3
"""Генератор навантаження з фіксованою інтенсивністю (відкритий контур).

Моменти відправки всіх запитів обчислюються наперед від спільного t0:
запит i процесу p має запланований час  t0 + (i + p/P) / (rate/P).
Цикл планування лише чекає на цей час і віддає запит пулу потоків, тому
повільні відповіді не затримують наступні відправки.

Затримка рахується від ЗАПЛАНОВАНОГО моменту (а не від фактичної відправки):
якщо сам генератор запізнюється, це видно у затримці та в полі lag_*.

Режим --mode closed: фіксована кількість одночасних запитів, наступний
запит іде після відповіді на попередній (закритий контур).
"""
import argparse
import csv
import gzip
import http.client
import json
import math
import multiprocessing as mp
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse


def _percentile(sorted_vals, q):
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return sorted_vals[lo]
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


class Client:
    """По одному постійному зʼєднанню на потік."""

    def __init__(self, host, port, timeout):
        self.host, self.port, self.timeout = host, port, timeout
        self.local = threading.local()

    def get(self, path):
        conn = getattr(self.local, "conn", None)
        if conn is None:
            conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
            self.local.conn = conn
        try:
            conn.request("GET", path)
            resp = conn.getresponse()
            resp.read()
            served = resp.getheader("X-Served-By") or ""
            if resp.getheader("Connection", "").lower() == "close":
                conn.close()
                self.local.conn = None
            return resp.status, served
        except TimeoutError:
            status = -1  # таймаут
        except Exception:  # noqa: BLE001
            status = 0  # помилка зʼєднання
        try:
            conn.close()
        finally:
            self.local.conn = None
        return status, ""


def make_path(template, ids, rng):
    return template.format(id=rng.choice(ids)) if ids else template


def open_worker(p, procs, rate_total, t0, total_s, a, ids, q):
    rng = random.Random(1000 + p)
    rate_p = rate_total / procs
    n = int(round(rate_p * total_s))
    client = Client(a.host, a.port, a.timeout)
    out = []

    def task(sched, path):
        start = time.monotonic()
        status, served = client.get(path)
        end = time.monotonic()
        out.append((sched, start, end, status, served))  # list.append потокобезпечний

    threads = math.ceil(rate_p * a.timeout) + 16
    with ThreadPoolExecutor(max_workers=threads) as pool:
        for i in range(n):
            sched = t0 + (i + p / procs) / rate_p
            delay = sched - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            pool.submit(task, sched, make_path(a.path, ids, rng))
    q.put(out)


def closed_worker(p, conc_p, t0, total_s, a, ids, q):
    rng = random.Random(2000 + p)
    client = Client(a.host, a.port, a.timeout)
    out = []
    stop = t0 + total_s

    def loop():
        while time.monotonic() < stop:
            sched = time.monotonic()
            status, served = client.get(make_path(a.path, ids, rng))
            end = time.monotonic()
            out.append((sched, sched, end, status, served))

    ts = [threading.Thread(target=loop) for _ in range(conc_p)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    q.put(out)


def summarize(rows, t0, warmup, duration):
    w0, w1 = t0 + warmup, t0 + warmup + duration
    win = [r for r in rows if w0 <= r[0] < w1]
    lat = sorted(r[2] - r[0] for r in win)
    lat_ok = sorted(r[2] - r[0] for r in win if 200 <= r[3] < 300)
    lag = sorted(r[1] - r[0] for r in win)
    ok_win = sum(1 for r in win if 200 <= r[3] < 300)
    done_ok = sum(1 for r in rows if w0 <= r[2] < w1 and 200 <= r[3] < 300)
    done_all = sum(1 for r in rows if w0 <= r[2] < w1)
    # L = середнє число запитів у системі: інтеграл N(t) за вікном / довжина вікна
    area = sum(max(0.0, min(r[2], w1) - max(r[0], w0)) for r in rows)
    served = {}
    for r in win:
        served[r[4] or "-"] = served.get(r[4] or "-", 0) + 1
    n = len(win)
    mean_w = statistics.fmean(r[2] - r[0] for r in win) if win else float("nan")
    ms = lambda v: round(v * 1000, 3)  # noqa: E731
    return {
        "requests_in_window": n,
        "offered_rps": round(n / duration, 3),
        "achieved_rps": round(done_ok / duration, 3),          # успішні (2xx) відповіді за секунду
        "completed_rps_any_status": round(done_all / duration, 3),
        "p50_ms": ms(_percentile(lat, 0.50)),
        "p95_ms": ms(_percentile(lat, 0.95)),
        "p99_ms": ms(_percentile(lat, 0.99)),
        "p50_ok_ms": ms(_percentile(lat_ok, 0.50)),
        "p95_ok_ms": ms(_percentile(lat_ok, 0.95)),
        "p99_ok_ms": ms(_percentile(lat_ok, 0.99)),
        "non2xx_share": round(1 - ok_win / n, 5) if n else float("nan"),
        "status_counts": {str(s): sum(1 for r in win if r[3] == s) for s in sorted({r[3] for r in win})},
        "mean_latency_ms": ms(mean_w),
        "little": {
            "lambda_rps": round(n / duration, 3),
            "W_s": round(mean_w, 6),
            "lambda_times_W": round(n / duration * mean_w, 4),
            "L_measured": round(area / duration, 4),
        },
        "generator_lag_p99_ms": ms(_percentile(lag, 0.99)),
        "generator_lag_max_ms": ms(lag[-1]) if lag else float("nan"),
        "served_by": served,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", required=True, help="база, напр. http://localhost:8080")
    ap.add_argument("--path", default="/studies/{id}", help="шаблон шляху; {id} береться з --ids")
    ap.add_argument("--ids", default=os.path.join(os.path.dirname(__file__), "ids.json"))
    ap.add_argument("--noop", action="store_true", help="самоперевірка: GET /noop, ідентифікатори не потрібні")
    ap.add_argument("--mode", choices=["open", "closed"], default="open")
    ap.add_argument("--rate", type=float, help="запитів/с (open)")
    ap.add_argument("--concurrency", type=int, help="одночасних запитів (closed)")
    ap.add_argument("--duration", type=float, required=True, help="тривалість вимірювання, с")
    ap.add_argument("--warmup", type=float, required=True, help="прогрів, с (у результат не входить)")
    ap.add_argument("--procs", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--timeout", type=float, required=True, help="таймаут одного запиту, с")
    ap.add_argument("--name", required=True, help="імʼя прогону; файли results/<name>.json і raw/<name>.csv.gz")
    ap.add_argument("--results-dir", default=os.path.join(os.path.dirname(__file__), "..", "results"))
    a = ap.parse_args()

    u = urlparse(a.url)
    a.host, a.port = u.hostname, u.port or 80
    if a.noop:
        a.path, ids = "/noop", []
    else:
        a.path = a.path
        with open(a.ids) as f:
            ids = json.load(f)
        assert ids, "порожній список ідентифікаторів: спершу виконайте seed.py"
    if a.mode == "open" and not a.rate:
        ap.error("--rate обовʼязковий для open")
    if a.mode == "closed" and not a.concurrency:
        ap.error("--concurrency обовʼязковий для closed")

    total = a.warmup + a.duration
    procs = max(1, min(a.procs, int(a.rate) if a.mode == "open" else a.concurrency))
    q = mp.Queue()
    t0 = time.monotonic() + 1.0  # 1 с на запуск процесів
    ps = []
    for p in range(procs):
        if a.mode == "open":
            args = (p, procs, a.rate, t0, total, a, ids, q)
            ps.append(mp.Process(target=open_worker, args=args))
        else:
            conc_p = a.concurrency // procs + (1 if p < a.concurrency % procs else 0)
            ps.append(mp.Process(target=closed_worker, args=(p, conc_p, t0, total, a, ids, q)))
    for pr in ps:
        pr.start()
    rows = []
    for _ in ps:
        rows.extend(q.get())
    for pr in ps:
        pr.join()

    res = summarize(rows, t0, a.warmup, a.duration)
    res["config"] = {
        "name": a.name, "mode": a.mode, "url": a.url, "path": a.path, "rate": a.rate,
        "concurrency": a.concurrency, "warmup_s": a.warmup, "duration_s": a.duration,
        "procs": procs, "timeout_s": a.timeout, "noop": a.noop, "cpu_count": os.cpu_count(),
    }
    os.makedirs(os.path.join(a.results_dir, "raw"), exist_ok=True)
    with open(os.path.join(a.results_dir, f"{a.name}.json"), "w") as f:
        json.dump(res, f, indent=2, ensure_ascii=False)
    with gzip.open(os.path.join(a.results_dir, "raw", f"{a.name}.csv.gz"), "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(["sched_s", "start_s", "end_s", "status", "served_by"])
        for r in sorted(rows):
            w.writerow([round(r[0] - t0, 6), round(r[1] - t0, 6), round(r[2] - t0, 6), r[3], r[4]])

    print(
        f"{a.name}: offered={res['offered_rps']} achieved={res['achieved_rps']} rps  "
        f"p50={res['p50_ms']} p95={res['p95_ms']} p99={res['p99_ms']} ms  "
        f"non2xx={res['non2xx_share']*100:.2f}%  L={res['little']['L_measured']}  "
        f"gen_lag_p99={res['generator_lag_p99_ms']} ms  served_by={res['served_by']}"
    )


if __name__ == "__main__":
    main()
