#!/usr/bin/env python3
"""ЛР4: режим створення задач. Генератор з розімкненим циклом надсилає POST /studies/{id}/tasks із заданою
інтенсивністю для записів зі списку ідентифікаторів (зазвичай сто записів, занесених loadgen/seed.py) і записує
ідентифікатори створених задач. Інтенсивність не залежить від швидкості відповідей: запити, що не встигли,
накопичуються в черзі генератора, а їхнє відставання видно в полі lag_p99_ms підсумку.

Приклади:
  python loadgen/taskgen.py --url http://localhost:8080 --ids loadgen/ids.json --rate 100 --count 300 --out results/tasks_x.json
  python loadgen/taskgen.py --url ... --ids ... --rate 20 --duration 180 --out ...
  python loadgen/taskgen.py ... --rate 5 --count 100 --delay-min 10 --delay-max 60      # Д2: run_after через 10-60 с
"""
import argparse
import http.client
import json
import queue
import random
import statistics
import threading
import time
import urllib.parse


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--ids", required=True, help="JSON зі списком record_id")
    ap.add_argument("--rate", type=float, required=True, help="задач за секунду")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--count", type=int)
    g.add_argument("--duration", type=float, help="секунд")
    ap.add_argument("--out", required=True)
    ap.add_argument("--pool", type=int, default=16, help="кількість постійних зʼєднань")
    ap.add_argument("--delay-min", type=float, default=None, help="Д2: мінімальний delay_s")
    ap.add_argument("--delay-max", type=float, default=None, help="Д2: максимальний delay_s")
    ap.add_argument("--seed", type=int, default=1, help="seed вибору записів і затримок (відтворюваність)")
    a = ap.parse_args()

    ids = json.load(open(a.ids))
    u = urllib.parse.urlparse(a.url)
    rng = random.Random(a.seed)
    total = a.count if a.count is not None else int(round(a.rate * a.duration))
    jobs: "queue.Queue" = queue.Queue()
    results = []
    lock = threading.Lock()

    def sender():
        conn = None
        while True:
            job = jobs.get()
            if job is None:
                return
            idx, record_id, due, delay = job
            path = f"/studies/{record_id}/tasks" + (f"?delay_s={delay:.3f}" if delay is not None else "")
            sent = time.time()
            status, task, err = None, None, None
            for attempt in (1, 2):
                try:
                    if conn is None:
                        conn = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=15)
                    conn.request("POST", path, body=b"", headers={"Content-Length": "0"})
                    resp = conn.getresponse()
                    body = resp.read()
                    status = resp.status
                    if status == 202:
                        task = json.loads(body)
                    break
                except (http.client.HTTPException, OSError) as exc:
                    conn = None
                    err = repr(exc)
            done = time.time()
            with lock:
                results.append({"i": idx, "record_id": record_id, "due": due, "sent": sent, "done": done,
                                "status": status, "task_id": task["id"] if task else None,
                                "delay_s": delay, "error": err if status is None else None})

    threads = [threading.Thread(target=sender, daemon=True) for _ in range(a.pool)]
    for t in threads:
        t.start()

    t0 = time.time()
    for i in range(total):
        due = t0 + i / a.rate
        wait = due - time.time()
        if wait > 0.002:
            time.sleep(wait - 0.001)
        delay = None
        if a.delay_min is not None:
            delay = rng.uniform(a.delay_min, a.delay_max if a.delay_max is not None else a.delay_min)
        jobs.put((i, rng.choice(ids), due, delay))
    for _ in threads:
        jobs.put(None)
    for t in threads:
        t.join()
    t1 = time.time()

    results.sort(key=lambda r: r["i"])
    ok = [r for r in results if r["status"] == 202]
    lag = sorted((r["sent"] - r["due"]) * 1000 for r in results)
    codes = {}
    for r in results:
        k = str(r["status"]) if r["status"] is not None else "error"
        codes[k] = codes.get(k, 0) + 1
    summary = {
        "requested": total, "rate": a.rate, "created": len(ok), "non_2xx": len(results) - len(ok), "codes": codes,
        "t_start": t0, "t_end": t1, "achieved_rate": len(ok) / max(t1 - t0, 1e-9),
        "lag_p99_ms": lag[int(0.99 * (len(lag) - 1))] if lag else None,
        "latency_p50_ms": statistics.median((r["done"] - r["sent"]) * 1000 for r in results) if results else None,
        "task_ids": [r["task_id"] for r in ok],
    }
    json.dump({"summary": summary, "requests": results}, open(a.out, "w"))
    print(f"створено {summary['created']}/{total} задач, не 2xx: {summary['non_2xx']} {codes}, "
          f"інтенсивність {summary['achieved_rate']:.1f}/с, відставання p99 {summary['lag_p99_ms']:.1f} мс")


if __name__ == "__main__":
    main()
