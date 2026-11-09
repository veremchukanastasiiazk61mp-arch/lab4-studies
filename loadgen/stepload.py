
import argparse
import asyncio
import csv
import json
import math
import multiprocessing as mp
import os
import random
import statistics
import time
from collections import defaultdict
from urllib.parse import urlparse


def percentile(sorted_vals, q):
    """Лінійна інтерполяція між найближчими рангами; NaN для порожнього набору."""
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return sorted_vals[lo]
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


# ------------------------------------------------------------------ профіль

def rate_at(sec, warmup, step_s, step_rate):
    """Задана інтенсивність у секунді sec від початку прогону."""
    if sec < warmup:
        return step_rate
    return step_rate * ((sec - warmup) // step_s + 1)


def profile_total(profile):
    return sum(d for d, _ in profile)


def rate_of(a, sec):
    """Задана інтенсивність у секунді sec: за профілем (--profile) або ступінчаста."""
    if a.profile:
        acc = 0
        for dur, rate in a.profile:
            acc += dur
            if sec < acc:
                return rate
        return a.profile[-1][1]
    return rate_at(sec, a.warmup, a.step_seconds, a.step_rate)


def segment_of(profile, sec):
    """Номер відрізка профілю (з 1) для секунди sec."""
    acc = 0
    for i, (dur, _) in enumerate(profile, 1):
        acc += dur
        if sec < acc:
            return i
    return len(profile)


def step_of(sec, warmup, step_s):
    """0 — прогрів, k ≥ 1 — номер сходинки."""
    return 0 if sec < warmup else (sec - warmup) // step_s + 1


# ------------------------------------------------------------------ HTTP-клієнт

class Pool:
    """Пул постійних зʼєднань HTTP/1.1; кількість зʼєднань обмежена семафором."""

    def __init__(self, host, port, max_conns, timeout, reqs_per_conn):
        self.host, self.port, self.timeout = host, port, timeout
        self.reqs_per_conn = reqs_per_conn   # ЛР3: зʼєднання закривається після стільки запитів
        self.idle = []
        self.sem = asyncio.Semaphore(max_conns)

    async def get(self, path):
        """Повертає (status, pod): status = код відповіді, -1 тайм-аут, 0 помилка зʼєднання."""
        try:
            return await asyncio.wait_for(self._get(path), self.timeout)
        except asyncio.TimeoutError:
            return -1, ""
        except (OSError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, ValueError, IndexError):
            return 0, ""

    async def _get(self, path):
        request = f"GET {path} HTTP/1.1\r\nHost: {self.host}:{self.port}\r\nConnection: keep-alive\r\n\r\n".encode()
        async with self.sem:
            for attempt in (0, 1):
                reused = bool(self.idle)
                if reused:
                    reader, writer, used = self.idle.pop()
                else:
                    reader, writer = await asyncio.open_connection(self.host, self.port)
                    used = 0
                keep = False
                try:
                    writer.write(request)
                    status, keep, pod = await _read_response(reader)
                    used += 1
                    if used >= self.reqs_per_conn:
                        keep = False   # ЛР3: 100 запитів на зʼєднання, далі нове зʼєднання
                    return status, pod
                except (asyncio.IncompleteReadError, ConnectionError):
                    # сервер закрив простоювале зʼєднання до відповіді: один повтор на новому зʼєднанні
                    if reused and attempt == 0:
                        continue
                    raise
                finally:
                    if keep:
                        self.idle.append((reader, writer, used))
                    else:
                        writer.close()


async def _read_response(reader):
    head = await reader.readuntil(b"\r\n\r\n")
    lines = head.split(b"\r\n")
    status = int(lines[0].split()[1])
    headers = {}
    for line in lines[1:]:
        if b":" in line:
            k, v = line.split(b":", 1)
            headers[k.strip().lower()] = v.strip()
    keep = headers.get(b"connection", b"").lower() != b"close"
    if status in (204, 304) or 100 <= status < 200:
        pass
    elif headers.get(b"transfer-encoding", b"").lower() == b"chunked":
        while True:
            size = int((await reader.readline()).split(b";")[0].strip() or b"0", 16)
            if size == 0:
                await reader.readuntil(b"\r\n")
                break
            await reader.readexactly(size + 2)
    elif b"content-length" in headers:
        await reader.readexactly(int(headers[b"content-length"]))
    else:
        await reader.read()  # тіло до закриття зʼєднання
        keep = False
    return status, keep, headers.get(b"x-pod", b"").decode("ascii", "replace")


# ------------------------------------------------------------------ робочий процес

def worker(p, procs, a, ids, t0, end_s, ok_shared, maxsec, q):
    asyncio.run(_worker(p, procs, a, ids, t0, end_s, ok_shared, maxsec, q))


async def _worker(p, procs, a, ids, t0, end_s, ok_shared, maxsec, q):
    rng = random.Random(1000 + p)
    pool = Pool(a.host, a.port, a.max_conns, a.timeout, a.reqs_per_conn)
    sent = defaultdict(int)
    ok = defaultdict(int)
    non2xx = defaultdict(int)
    conn_err = defaultdict(int)
    lat = defaultdict(list)
    pods = defaultdict(int)
    lag = []
    tasks = set()

    async def one(sched, path):
        start = time.monotonic()
        sent[int(start - t0)] += 1
        lag.append(start - sched)
        status, pod = await pool.get(path)
        end = time.monotonic()
        s = int(end - t0)
        lat[s].append(end - sched)
        if 200 <= status < 300:
            ok[s] += 1
            pods[pod or "-"] += 1
            if s < maxsec:
                ok_shared[p * maxsec + s] += 1
        elif status == 0:
            conn_err[s] += 1
        else:
            non2xx[s] += 1

    # Кожен процес подає 1/procs загальної інтенсивності; фази зсунуті, щоб запити
    # процесів чергувались, а не надходили пачками.
    tau = p / rate_of(a, 0)
    max_tau = profile_total(a.profile) if a.profile else a.warmup + a.step_seconds * math.ceil(a.max_rate / a.step_rate)
    n = 0
    while True:
        end = end_s.value
        if (end > 0 and tau >= end) or tau >= max_tau:
            break
        total_rate = rate_of(a, int(tau))
        delay = (t0 + tau) - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        path = a.path.format(id=rng.choice(ids)) if ids else a.path
        task = asyncio.ensure_future(one(t0 + tau, path))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        tau += procs / total_rate
        n += 1
        if delay <= 0 and n % 32 == 0:
            await asyncio.sleep(0)
    if tasks:
        await asyncio.wait(list(tasks), timeout=a.timeout + 2)
    q.put({
        "sent": dict(sent), "ok": dict(ok), "non2xx": dict(non2xx), "conn_err": dict(conn_err),
        "lat": {s: v for s, v in lat.items()}, "pods": dict(pods),
        "lag": sorted(lag)[int(0.99 * (len(lag) - 1))] if lag else 0.0,
    })


# ------------------------------------------------------------------ підсумки

def per_second_table(parts, a, end_s):
    """Зводить дані процесів у посекундну таблицю."""
    sent, ok, non2xx, conn_err = (defaultdict(int) for _ in range(4))
    lat = defaultdict(list)
    for part in parts:
        for key, dst in (("sent", sent), ("ok", ok), ("non2xx", non2xx), ("conn_err", conn_err)):
            for s, v in part[key].items():
                dst[int(s)] += v
        for s, v in part["lat"].items():
            lat[int(s)].extend(v)
    last = max(list(sent) + list(ok) + list(non2xx) + list(conn_err) + [int(end_s)])
    rows = []
    warmup, step_s = a.warmup, a.step_seconds
    for s in range(0, last + 1):
        l = sorted(lat.get(s, []))
        in_run = s < end_s
        if a.profile:
            phase = f"seg{segment_of(a.profile, s)}" if in_run else "drain"
            step = segment_of(a.profile, s) if in_run else -1
        else:
            phase = ("warmup" if s < warmup else f"step{step_of(s, warmup, step_s)}") if in_run else "drain"
            step = step_of(s, warmup, step_s) if in_run else -1
        rows.append({
            "t_s": s,
            "phase": phase,
            "step": step,
            "offered_rps": rate_of(a, s) if in_run else 0,
            "sent_rps": sent.get(s, 0),
            "achieved_rps": ok.get(s, 0),
            "ok_2xx": ok.get(s, 0),
            "non_2xx": non2xx.get(s, 0),
            "conn_errors": conn_err.get(s, 0),
            "p50_ms": round(percentile(l, 0.50) * 1000, 3) if l else "",
            "p95_ms": round(percentile(l, 0.95) * 1000, 3) if l else "",
            "p99_ms": round(percentile(l, 0.99) * 1000, 3) if l else "",
        })
    return rows, lat


def segment_summaries(rows, lat, profile, transient):
    """Підсумок кожного відрізка профілю; середні за вікном [початок + transient, кінець)."""
    out, start = [], 0
    for i, (dur, rate) in enumerate(profile, 1):
        win = [r for r in rows if start + transient <= r["t_s"] < start + dur]
        whole = [r for r in rows if start <= r["t_s"] < start + dur]
        lats = sorted(x for r in win for x in lat.get(r["t_s"], []))
        ach = [r["achieved_rps"] for r in win]
        out.append({
            "segment": i, "duration_s": dur, "offered_rps": rate,
            "achieved_mean_rps": round(statistics.fmean(ach), 3) if ach else None,
            "p99_ms": round(percentile(lats, 0.99) * 1000, 3) if lats else None,
            "non_2xx": sum(r["non_2xx"] for r in whole),
            "conn_errors": sum(r["conn_errors"] for r in whole),
            "window_s": [start + transient, start + dur],
        })
        start += dur
    return out


def step_summaries(rows, lat, warmup, step_s, transient, step_rate, n_steps):
    """Підсумок кожної сходинки за вікном [transient, step_s) секунд сходинки."""
    out = []
    for k in range(1, n_steps + 1):
        start = warmup + (k - 1) * step_s
        win = [r for r in rows if start + transient <= r["t_s"] < start + step_s]
        if not win:
            break
        ach = [r["achieved_rps"] for r in win]
        lats = sorted(x for r in win for x in lat.get(r["t_s"], []))
        done = sum(r["ok_2xx"] + r["non_2xx"] + r["conn_errors"] for r in win)
        offered = step_rate * k
        mean = statistics.fmean(ach)
        out.append({
            "step": k,
            "offered_rps": offered,
            "achieved_mean_rps": round(mean, 3),
            "achieved_std_rps": round(statistics.stdev(ach), 3) if len(ach) > 1 else 0.0,
            "sent_mean_rps": round(statistics.fmean(r["sent_rps"] for r in win), 3),
            "ratio": round(mean / offered, 4),
            "non_2xx_share": round(1 - sum(r["ok_2xx"] for r in win) / done, 5) if done else None,
            "conn_errors": sum(r["conn_errors"] for r in win),
            "p50_ms": round(percentile(lats, 0.50) * 1000, 3) if lats else None,
            "p95_ms": round(percentile(lats, 0.95) * 1000, 3) if lats else None,
            "p99_ms": round(percentile(lats, 0.99) * 1000, 3) if lats else None,
            "window_s": [start + transient, start + step_s],
        })
    return out


# ------------------------------------------------------------------ головна програма

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", required=True, help="база, напр. http://localhost:8080")
    ap.add_argument("--path", default="/studies/{id}", help="шаблон шляху; {id} береться з --ids")
    ap.add_argument("--ids", default=os.path.join(os.path.dirname(__file__), "ids.json"))
    ap.add_argument("--noop", action="store_true", help="прогін проти /noop (крок 6): ідентифікатори не потрібні")
    ap.add_argument("--name", required=True, help="імʼя прогону; файли <results-dir>/<name>.json і <name>_seconds.csv")
    ap.add_argument("--g", type=float, help="межа генератора G, запитів/с: зупинитись після сходинки, що її досягла")
    ap.add_argument("--max-rate", type=int, default=20000, help="запобіжник: найбільша задана інтенсивність, запитів/с")
    ap.add_argument("--warmup", type=int, default=10, help="прогрів, с (на рівні --step-rate)")
    ap.add_argument("--step-rate", type=int, default=200, help="початкова інтенсивність і приріст сходинки")
    ap.add_argument("--step-seconds", type=int, default=30, help="тривалість сходинки, с (ЛР3: 30)")
    ap.add_argument("--transient", type=int, default=10, help="перехідні перші секунди сходинки, с (ЛР3: середнє за останні 20 с)")
    ap.add_argument("--timeout", type=float, default=5.0, help="тайм-аут запиту, с")
    ap.add_argument("--procs", type=int, default=max(1, (os.cpu_count() or 2) // 2),
                    help="процесів генератора; у всіх прогонах серії значення має бути однаковим")
    ap.add_argument("--conns", type=int, default=40, help="ЛР3: усього постійних зʼєднань генератора (8 · R; для R = 5 це 40)")
    ap.add_argument("--reqs-per-conn", type=int, default=100, help="ЛР3: закривати зʼєднання після стількох запитів")
    ap.add_argument("--profile", help='ЛР3, крок 10: відрізки "тривалість:інтенсивність" через кому, напр. "30:75,60:610,90:75"')
    ap.add_argument("--results-dir", default=os.path.join(os.path.dirname(__file__), "..", "results"))
    a = ap.parse_args()

    u = urlparse(a.url)
    a.host, a.port = u.hostname, u.port or 80
    a.max_conns = max(1, round(a.conns / a.procs))   # зʼєднань на процес; усього ≈ a.conns
    a.profile = [(int(d), int(r)) for d, r in (seg.split(":") for seg in a.profile.split(","))] if a.profile else None
    if a.noop:
        a.path, ids = "/noop", []
    else:
        with open(a.ids) as f:
            ids = json.load(f)
        assert ids, "порожній список ідентифікаторів: спершу виконайте seed.py"
    assert a.transient < a.step_seconds

    max_steps = math.ceil(a.max_rate / a.step_rate)
    maxsec = int(a.warmup + max_steps * a.step_seconds + a.timeout + 10)
    ok_shared = mp.RawArray("i", a.procs * maxsec)
    end_s = mp.RawValue("d", -1.0)
    q = mp.Queue()
    t0 = time.monotonic() + 1.5  # час на запуск процесів
    t0_unix = time.time() + 1.5  # той самий момент за настінним годинником: для звірки з Prometheus і docker stop
    print(f"T0_UNIX={t0_unix:.3f}", flush=True)
    ps = [mp.Process(target=worker, args=(p, a.procs, a, ids, t0, end_s, ok_shared, maxsec, q)) for p in range(a.procs)]
    for pr in ps:
        pr.start()

    below = 0
    reason = "max_rate"
    k = 0
    if a.profile:
        # ЛР3, крок 10: профіль із відрізків; прогін триває весь профіль, зупинки за сходинками немає
        step_end = profile_total(a.profile)
        wait = (t0 + step_end + 0.3) - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        reason = "profile_finished"
    else:
        while True:
            k += 1
            step_start = a.warmup + (k - 1) * a.step_seconds
            step_end = a.warmup + k * a.step_seconds
            wait = (t0 + step_end + 0.3) - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            offered = a.step_rate * k
            lo, hi = step_start + a.transient, step_end
            mean = sum(ok_shared[p * maxsec + s] for p in range(a.procs) for s in range(lo, hi)) / (hi - lo)
            print(f"  сходинка {k}: задано {offered}, досягнуто {mean:.1f} rps ({mean / offered:.0%})", flush=True)
            below = below + 1 if mean < 0.9 * offered else 0
            if below >= 2:
                reason = "two_steps_below_0.9"
                break
            if a.g is not None and mean >= a.g:
                reason = "generator_limit_G_reached"
                break
            if offered >= a.max_rate:
                reason = "max_rate"
                break
    end_s.value = float(step_end)
    parts = [q.get() for _ in ps]
    for pr in ps:
        pr.join()

    rows, lat = per_second_table(parts, a, step_end)
    if a.profile:
        steps = segment_summaries(rows, lat, a.profile, a.transient)
        best = {"achieved_mean_rps": None, "achieved_std_rps": None, "step": None}
    else:
        steps = step_summaries(rows, lat, a.warmup, a.step_seconds, a.transient, a.step_rate, k)
        best = max(steps, key=lambda s: s["achieved_mean_rps"])
    pods = defaultdict(int)
    for part in parts:
        for pod, v in part["pods"].items():
            pods[pod] += v
    result = {
        "X_rps": best["achieved_mean_rps"],
        "X_std_rps": best["achieved_std_rps"],
        "X_step": best["step"],
        "stop_reason": reason,
        "steps": steps,
        "responses_2xx_by_pod": dict(pods),
        "generator_lag_p99_ms": round(max(part["lag"] for part in parts) * 1000, 3),
        "config": {
            "name": a.name, "url": a.url, "path": a.path, "noop": a.noop, "warmup_s": a.warmup,
            "step_rate": a.step_rate, "step_seconds": a.step_seconds, "transient_s": a.transient,
            "timeout_s": a.timeout, "procs": a.procs, "max_conns": a.max_conns, "g": a.g,
            "max_rate": a.max_rate, "cpu_count": os.cpu_count(),
            "conns": a.conns, "conns_per_proc": a.max_conns, "reqs_per_conn": a.reqs_per_conn, "profile": a.profile,
            "t0_unix": round(t0_unix, 3),  # початок прогону (прогрів) за настінним годинником
        },
    }
    os.makedirs(a.results_dir, exist_ok=True)
    with open(os.path.join(a.results_dir, f"{a.name}.json"), "w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    with open(os.path.join(a.results_dir, f"{a.name}_seconds.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"{a.name}: X={result['X_rps']} ± {result['X_std_rps']} rps (сходинка {result['X_step']}), "
          f"зупинка: {reason}, відставання планувальника p99 {result['generator_lag_p99_ms']} мс")


if __name__ == "__main__":
    main()
