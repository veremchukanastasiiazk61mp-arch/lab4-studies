#!/usr/bin/env python3
"""ЛР2, кроки 7–11: розрахунки і діаграми за результатами серії.

Читає з results/ (їх створює scripts/run_series.py):
  <тег>manifest.json, <тег>noop.json, <тег>step_nN.json + _seconds.csv + _docker_stats.jsonl,
  <тег>prom_nN.json (сирі вибірки Prometheus).
Пише:
  results/<тег>analysis.json   усі обчислені величини
  results/<тег>analysis.md     таблиці для звіту
  results/figures/*.png        діаграми кроків 8, 10, 11

Запуск з кореня проєкту:  python analysis/analyze.py [--tag cpu025_]
Потрібно: pip install matplotlib
"""
import argparse
import csv
import json
import math
import os
import statistics
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from usl import fit_usl, quantile_from_buckets, usl  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PATH_TEMPLATE = "/studies/{study_id}"
WINDOW_S = 15  # вікно p99 сервера на діаграмі кроку 8
GRID_S = 5     # крок оновлення p99 сервера


# ------------------------------------------------------------------ допоміжне

def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_seconds(path):
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            row = {k: (float(v) if v not in ("", None) else None) for k, v in r.items() if k != "phase"}
            row["phase"] = r["phase"]
            rows.append(row)
    return rows


def fmt(x, d=1):
    """Число з десятковою комою, як у звіті."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "—"
    text = f"{x:.{d}f}"
    if float(text) == 0:  # без «-0,0»
        text = text.lstrip("-")
    return text.replace(".", ",")


def interp(vals, t):
    """Значення лічильника в момент t: лінійна інтерполяція між сусідніми вибірками."""
    if not vals:
        return 0.0
    if t <= vals[0][0]:
        return vals[0][1]
    if t >= vals[-1][0]:
        return vals[-1][1]
    lo, hi = 0, len(vals) - 1
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if vals[mid][0] <= t:
            lo = mid
        else:
            hi = mid
    (t0, v0), (t1, v1) = vals[lo], vals[hi]
    return v0 + (v1 - v0) * (t - t0) / (t1 - t0) if t1 > t0 else v1


def counter_delta(vals, t0, t1):
    """Приріст лічильника за [t0, t1]; перезапуск процесу (спад значення) не дає відʼємного приросту."""
    pts = [(t0, interp(vals, t0))] + [p for p in vals if t0 < p[0] < t1] + [(t1, interp(vals, t1))]
    total = 0.0
    for (_, a), (_, b) in zip(pts, pts[1:]):
        total += b - a if b >= a else b
    return total


def gauge_mean(vals, t0, t1):
    inside = [v for t, v in vals if t0 <= t <= t1]
    return statistics.fmean(inside) if inside else interp(vals, (t0 + t1) / 2)


# ------------------------------------------------------------------ один прогін

class Run:
    def __init__(self, res, tag, n):
        self.n = n
        self.name = f"{tag}step_n{n}"
        self.summary = load_json(os.path.join(res, f"{self.name}.json"))
        self.rows = load_seconds(os.path.join(res, f"{self.name}_seconds.csv"))
        self.prom = load_json(os.path.join(res, f"{tag}prom_n{n}.json"))
        cfg = self.summary["config"]
        self.t0 = cfg["t0_unix"]
        self.warmup, self.step_s = cfg["warmup_s"], cfg["step_seconds"]
        self.transient, self.step_rate = cfg["transient_s"], cfg["step_rate"]
        off = self.prom["clock_offset_s"]  # хост − Prometheus: переводить мітки Prometheus у час хоста
        self.series = {
            name: [(s["metric"], [(t + off, v) for t, v in s["values"]]) for s in lst]
            for name, lst in self.prom["series"].items()
        }
        self.instances = sorted({m.get("instance") for m, _ in self.series.get("cpu_seconds", [])})
        self.steps = self.summary["steps"]
        self.end_s = max(r["t_s"] for r in self.rows if r["phase"] != "drain") + 1

    # --- вікно «останні 15 с сходинки»
    def window(self, k):
        start = self.t0 + self.warmup + (k - 1) * self.step_s
        return start + self.transient, start + self.step_s

    def x_step(self):
        return self.summary["X_step"]

    # --- гістограма сервера
    def buckets(self, w0, w1):
        inc = defaultdict(float)
        for m, vals in self.series.get("bucket", []):
            if m.get("method") == "GET" and m.get("path") == PATH_TEMPLATE:
                inc[m["le"]] += counter_delta(vals, w0, w1)
        finite = sorted((float(le), c) for le, c in inc.items() if le != "+Inf")
        return [b for b, _ in finite], [c for _, c in finite], inc.get("+Inf", 0.0)

    def server_p99(self, w0, w1):
        bounds, cum, total = self.buckets(w0, w1)
        return quantile_from_buckets(bounds, cum, total, 0.99), total

    # --- ресурси екземплярів
    def per_instance(self, name, w0, w1, kind, label_filter=None):
        out = {}
        for m, vals in self.series.get(name, []):
            if label_filter and not label_filter(m):
                continue
            inst = m.get("instance")
            v = counter_delta(vals, w0, w1) / (w1 - w0) if kind == "rate" else gauge_mean(vals, w0, w1)
            out[inst] = out.get(inst, 0.0) + v
        return out

    def requests_ok(self, w0, w1):
        return self.per_instance(
            "requests", w0, w1, "rate",
            lambda m: m.get("path") == PATH_TEMPLATE and m.get("method") == "GET" and str(m.get("code", "")).startswith("2"),
        )

    # --- М2
    def pg_states(self, w0, w1):
        return {m["state"]: gauge_mean(vals, w0, w1) for m, vals in self.series.get("pg_conn", [])}

    def pg_tps(self, w0, w1, db):
        total = 0.0
        for name in ("pg_xact_commit", "pg_xact_rollback"):
            for m, vals in self.series.get(name, []):
                if db is None or m.get("datname") == db:
                    total += counter_delta(vals, w0, w1) / (w1 - w0)
        return total

    def pg_numbackends(self, w0, w1, db):
        return sum(gauge_mean(vals, w0, w1) for m, vals in self.series.get("pg_numbackends", [])
                   if db is None or m.get("datname") == db)

    # --- метрики сходинки
    def step_metrics(self, k, limit_cpu, db):
        w0, w1 = self.window(k)
        st = self.steps[k - 1]
        q, total = self.server_p99(w0, w1)
        cpu = self.per_instance("cpu_seconds", w0, w1, "rate")
        infl = self.per_instance("in_flight", w0, w1, "gauge")
        reqs = self.requests_ok(w0, w1)
        cpu_mean = statistics.fmean(cpu.values()) if cpu else None
        return {
            "step": k, "offered_rps": st["offered_rps"], "X_mean_rps": st["achieved_mean_rps"],
            "X_std_rps": st["achieved_std_rps"], "ratio": st["ratio"],
            "p50_client_ms": st["p50_ms"], "p95_client_ms": st["p95_ms"], "p99_client_ms": st["p99_ms"],
            "non_2xx_share": st["non_2xx_share"], "conn_errors": st["conn_errors"],
            "server_p99": None if q is None else {
                "estimate_ms": q["estimate"] * 1000, "lower_ms": q["lower"] * 1000,
                "upper_ms": None if q["upper"] is None else q["upper"] * 1000,
                "max_error_ms": None if q["max_error"] is None else q["max_error"] * 1000,
                "beyond": q["beyond"], "n_requests": total,
            },
            "cpu_cores_per_instance": cpu_mean,
            "cpu_share_of_limit": None if cpu_mean is None else cpu_mean / limit_cpu,
            "inflight_per_instance": statistics.fmean(infl.values()) if infl else None,
            "req_per_instance": [reqs[i] for i in sorted(reqs)],
            "pg_states": self.pg_states(w0, w1),
            "pg_tps": self.pg_tps(w0, w1, db),
            "pg_numbackends": self.pg_numbackends(w0, w1, db),
        }


# ------------------------------------------------------------------ docker stats

def parse_mem_mib(text):
    used = text.split("/")[0].strip()
    for unit, mult in (("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024), ("kB", 1 / 1024), ("MB", 1), ("GB", 1024), ("B", 1 / 1048576)):
        if used.endswith(unit):
            return float(used[: -len(unit)]) * mult
    return float("nan")


def group_of(name):
    for key, g in (("-web-", "web"), ("-db-", "db"), ("-lb-", "lb"), ("postgres-exporter", "postgres-exporter"),
                   ("prometheus", "prometheus")):
        if key in name:
            return g
    return "інші"


def docker_stats_window(path, w0, w1):
    """Середнє за вибірками docker stats, що потрапили у вікно; CPU — у ядрах (100% = 1 ядро)."""
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        recs = [json.loads(x) for x in f if x.strip()]
    inside = [r for r in recs if w0 <= (r["ts"] + r["ts_end"]) / 2 <= w1] or []
    if not inside and recs:  # вибірки рідкі: беремо найближчу до середини вікна
        mid = (w0 + w1) / 2
        inside = [min(recs, key=lambda r: abs((r["ts"] + r["ts_end"]) / 2 - mid))]
    cpu, mem = defaultdict(list), defaultdict(list)
    for r in inside:
        per_group_cpu, per_group_mem = defaultdict(list), defaultdict(list)
        for c in r["containers"]:
            g = group_of(c.get("Name", ""))
            per_group_cpu[g].append(float(c["CPUPerc"].rstrip("%")) / 100)
            per_group_mem[g].append(parse_mem_mib(c["MemUsage"]))
        for g in per_group_cpu:
            cpu[g].append(statistics.fmean(per_group_cpu[g]))  # середнє на один контейнер групи
            mem[g].append(statistics.fmean(per_group_mem[g]))
    host = [r["host_cpu_percent"] for r in inside if "host_cpu_percent" in r]
    gen = [r["gen_cpu_percent"] for r in inside if "gen_cpu_percent" in r]
    hmem = [r["host_mem_percent"] for r in inside if "host_mem_percent" in r]
    return {
        "samples": len(inside),
        "groups": {g: {"cpu_cores": statistics.fmean(cpu[g]), "mem_mib": statistics.fmean(mem[g])} for g in cpu},
        "host_cpu_percent": statistics.fmean(host) if host else None,
        "host_mem_percent": statistics.fmean(hmem) if hmem else None,
        "generator_cpu_cores": statistics.fmean(gen) / 100 if gen else None,
    }


# ------------------------------------------------------------------ діаграми

INK, INK2, SURFACE, GRID = "#0b0b0b", "#52514e", "#fcfcfb", "#e4e3df"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]  # слоти 1–4 валідованої палітри


def setup_matplotlib():
    import matplotlib  # noqa: PLC0415
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # noqa: PLC0415
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
        "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
        "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
        "font.size": 10, "axes.titlesize": 10.5, "axes.titleweight": "normal", "axes.titlelocation": "left",
        "legend.frameon": False, "lines.linewidth": 1.8, "lines.solid_capstyle": "round",
        "figure.dpi": 110, "savefig.dpi": 150,
    })
    return plt


def label_end(ax, x, y, text, color=INK2, dy=0):
    ax.annotate(text, (x, y), xytext=(6, dy), textcoords="offset points", fontsize=8.5, color=INK2, va="center")


def host_stats(path):
    """Завантаження хоста за psutil за весь прогін (вибірки docker_stats.jsonl): середнє і максимум."""
    cpu, mem = [], []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                r = json.loads(line)
                if r.get("host_cpu_percent") is not None:
                    cpu.append(r["host_cpu_percent"])
                if r.get("host_mem_percent") is not None:
                    mem.append(r["host_mem_percent"])
    except OSError:
        return None
    if not cpu or not mem:
        return None
    return {"samples": len(cpu), "cpu_mean": statistics.mean(cpu), "cpu_max": max(cpu),
            "mem_mean": statistics.mean(mem), "mem_min": min(mem), "mem_max": max(mem)}


def figure_step8(plt, run, out, limit_cpu, pool):
    """Крок 8: діаграми на спільній осі часу для одного прогону."""
    n = run.n
    end = run.end_s
    t = [r["t_s"] for r in run.rows]
    fig, axes = plt.subplots(5, 1, figsize=(10, 13), sharex=True, constrained_layout=True)
    fig.suptitle(f"Прогін на {n} екз. web: ступінчасте навантаження", x=0.01, ha="left", fontsize=12)
    xs = [r["t_s"] for r in run.rows if r["phase"] != "drain"]

    ax = axes[0]
    ax.step(xs, [r["offered_rps"] for r in run.rows if r["phase"] != "drain"], where="post", color=SERIES[0], label="задана")
    ax.plot(t, [r["achieved_rps"] for r in run.rows], color=SERIES[1], label="досягнута (2xx за секунду)")
    ax.set_ylabel("запитів за секунду")
    ax.set_title("Задана і досягнута інтенсивність")
    ax.legend(loc="upper left")

    ax = axes[1]
    cl = [(r["t_s"], r["p99_ms"]) for r in run.rows if r["p99_ms"] is not None]
    ax.plot([a for a, _ in cl], [b for _, b in cl], color=SERIES[0], label="p99 клієнта (за секунду)")
    grid = [run.t0 + g for g in range(0, int(end) + 1, GRID_S)]
    sx, sy = [], []
    for g in grid:
        q, _ = run.server_p99(g - WINDOW_S, g)
        if q is not None:
            sx.append(g - run.t0)
            sy.append(q["estimate"] * 1000)
    ax.step(sx, sy, where="post", color=SERIES[1], label=f"p99 сервера (вікно {WINDOW_S} с, оновлення {GRID_S} с)")
    ax.set_yscale("log")
    ax.set_ylabel("мс, логарифмічна шкала")
    ax.set_title("p99 клієнта і сервера")
    ax.legend(loc="upper left")

    def per_instance_series(ax, name, kind, ylabel, title, hline=None, hlabel=None, label_filter=None):
        for idx, inst in enumerate(run.instances):
            pts = []
            for m, vals in run.series.get(name, []):
                if m.get("instance") != inst or (label_filter and not label_filter(m)):
                    continue
                if kind == "rate":
                    for g in grid:
                        pts.append((g - run.t0, counter_delta(vals, g - WINDOW_S, g) / WINDOW_S))
                else:
                    pts.extend((tt - run.t0, v) for tt, v in vals if run.t0 - 5 <= tt <= run.t0 + end)
            if kind == "rate":  # кілька рядів (коди) одного екземпляра сумуємо
                agg = defaultdict(float)
                for x, v in pts:
                    agg[x] += v
                pts = sorted(agg.items())
            pts.sort()
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=SERIES[idx % 4], label=f"екземпляр {idx + 1}")
        if hline is not None:
            ax.axhline(hline, color=INK2, linewidth=1, linestyle="-", alpha=0.7)
            ax.annotate(hlabel, (0.995, hline), xycoords=("axes fraction", "data"), xytext=(0, 4),
                        textcoords="offset points", ha="right", fontsize=8.5, color=INK2)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.legend(loc="upper left", ncol=min(4, max(1, len(run.instances))))

    per_instance_series(axes[2], "requests", "rate", "запитів за секунду",
                        f"Запити за секунду на екземпляр (2xx, вікно {WINDOW_S} с)",
                        label_filter=lambda m: m.get("path") == PATH_TEMPLATE and m.get("method") == "GET"
                        and str(m.get("code", "")).startswith("2"))
    per_instance_series(axes[3], "cpu_seconds", "rate", "ядер CPU", f"Використання CPU екземпляром (вікно {WINDOW_S} с)",
                        hline=limit_cpu, hlabel=f"ліміт {fmt(limit_cpu, 2)} CPU")
    per_instance_series(axes[4], "in_flight", "gauge", "запитів", "Запитів в обробці на екземпляр",
                        hline=pool, hlabel=f"розмір пулу {pool}")
    axes[4].set_xlabel("секунд від початку прогону")
    axes[4].set_xlim(0, end + 2)
    fig.savefig(out)
    plt.close(fig)


def figure_usl(plt, out, x1, rows, fit, g, n_max, repeats=False):
    fig, ax = plt.subplots(figsize=(8, 5), constrained_layout=True)
    x_max = n_max
    if fit and fit.get("N_star"):
        x_max = max(n_max, min(math.ceil(fit["N_star"] * 1.3), n_max * 3))  # показати максимум кривої
    ns = [i / 10 for i in range(10, int(x_max * 10) + 11)]
    ax.plot(ns, [x1 * n for n in ns], color=INK2, linewidth=1.2, label=f"лінійне λN, λ = X(1) = {fmt(x1, 0)}")
    if fit:
        ax.plot(ns, [usl(n, x1, fit["sigma"], fit["kappa"]) for n in ns], color=SERIES[0],
                label=f"закон: σ = {fmt(fit['sigma'], 3)}, κ = {fmt(fit['kappa'], 4)}")
    used = [r for r in rows if not r["generator_limited"]]
    lim = [r for r in rows if r["generator_limited"]]
    if repeats:
        ax.errorbar([r["N"] for r in used], [r["X"] for r in used],
                    yerr=[[r["X"] - r["lo"] for r in used], [r["hi"] - r["X"] for r in used]], fmt="o",
                    color=SERIES[1], ecolor=SERIES[1], markersize=8, markeredgecolor=SURFACE, markeredgewidth=2,
                    capsize=4, label="медіана X(N); відрізок — від мінімуму до максимуму серед прогонів")
        ax.plot([r["N"] for r in used for _ in r["runs"]], [x for r in used for x in r["runs"]], linestyle="none",
                marker="o", markersize=4.5, markerfacecolor=SURFACE, markeredgecolor=SERIES[2], markeredgewidth=1.2,
                label="окремі прогони")
    else:
        ax.errorbar([r["N"] for r in used], [r["X"] for r in used], yerr=[r["std"] for r in used], fmt="o",
                    color=SERIES[1], ecolor=SERIES[1], markersize=8, markeredgecolor=SURFACE, markeredgewidth=2,
                    capsize=4, label="виміряно X(N) ± розкид")
    if lim:
        ax.errorbar([r["N"] for r in lim], [r["X"] for r in lim], yerr=[r["std"] for r in lim], fmt="o",
                    markerfacecolor=SURFACE, color=SERIES[1], markersize=8, capsize=4,
                    label="обмежено генератором (не входить у підбір)")
    if g:
        ax.axhline(g, color=INK2, linewidth=1, alpha=0.7)
        ax.annotate(f"межа генератора G = {fmt(g, 0)}", (0.995, g), xycoords=("axes fraction", "data"),
                    xytext=(0, 4), textcoords="offset points", ha="right", fontsize=8.5, color=INK2)
    ax.set_xlabel("кількість екземплярів N")
    ax.set_ylabel("X(N), запитів за секунду")
    ax.set_title("Пропускна здатність залежно від кількості екземплярів")
    ax.set_xticks(range(1, int(math.ceil(x_max)) + 1))
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper left")
    fig.savefig(out)
    plt.close(fig)


def figure_m2(plt, out, series, pool):
    """Крок 11: М2 залежно від заданої інтенсивності для найменшого і найбільшого N."""
    ns = sorted(series)
    sel = [ns[0], ns[-1]] if len(ns) > 1 else ns
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.6), constrained_layout=True)
    for ax, n in zip(axes[:2], sel):
        pts = series[n]
        x = [p["offered_rps"] for p in pts]
        ax.plot(x, [p["active"] for p in pts], color=SERIES[0], marker="o", markersize=6, markeredgecolor=SURFACE,
                markeredgewidth=1.5, label="active")
        ax.plot(x, [p["idle"] for p in pts], color=SERIES[1], marker="o", markersize=6, markeredgecolor=SURFACE,
                markeredgewidth=1.5, label="idle")
        cap = n * pool
        ax.axhline(cap, color=INK2, linewidth=1, alpha=0.7)
        ax.annotate(f"N · пул = {n} · {pool} = {cap}", (0.995, cap), xycoords=("axes fraction", "data"),
                    xytext=(0, 4), textcoords="offset points", ha="right", fontsize=8.5, color=INK2)
        ax.set_title(f"Зʼєднання з базою за станом, N = {n}")
        ax.set_xlabel("задана інтенсивність, запитів за секунду")
        ax.set_ylabel("зʼєднань")
        ax.set_ylim(bottom=0)
        ax.legend(loc="center left")
    ax = axes[2]
    for i, n in enumerate(sel):
        pts = series[n]
        ax.plot([p["offered_rps"] for p in pts], [p["tps"] for p in pts], color=SERIES[i], marker="o", markersize=6,
                markeredgecolor=SURFACE, markeredgewidth=1.5, label=f"N = {n}")
    ax.set_title("Транзакції в базі за секунду")
    ax.set_xlabel("задана інтенсивність, запитів за секунду")
    ax.set_ylabel("транзакцій за секунду (commit + rollback)")
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper left")
    fig.savefig(out)
    plt.close(fig)


# ------------------------------------------------------------------ головна програма

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tag", default="", help="префікс імен файлів серії, напр. cpu025_")
    ap.add_argument("--results", default=os.path.join(ROOT, "results"))
    ap.add_argument("--no-figures", action="store_true")
    ap.add_argument("--extra", action="append", default=[], metavar="ТЕКА::ТЕГ::НАЗВА",
                    help="додаткові прогони тих самих N (повтори): тека результатів, префікс імен і назва для звіту; "
                         "можна вказувати кілька разів. Для X(N) береться медіана за основною серією і всіма повторами")
    a = ap.parse_args()
    res, tag = a.results, a.tag

    manifest = load_json(os.path.join(res, f"{tag}manifest.json"))
    g = manifest["G_rps"]
    limits = manifest.get("web_limits", {})
    limit_cpu = float(limits.get("WEB_CPUS", "0.5"))
    pool = int(limits.get("DB_POOL_SIZE", "15"))
    db = limits.get("DB_NAME")
    ns = sorted(int(n) for n in manifest["runs"])
    runs = {n: Run(res, tag, n) for n in ns}

    # ---- крок 7: X(N), розкид, сходинка, показники сходинки
    points = []
    for n, run in runs.items():
        k = run.x_step()
        m = run.step_metrics(k, limit_cpu, db)
        last = run.steps[-1]
        reasons = []
        if m["X_mean_rps"] >= 0.9 * g:
            reasons.append("X(N) ≥ 0,9·G")
        if last["ratio"] >= 0.9:
            reasons.append("на останній сходинці досягнуто ≥ 0,9 заданої")
        points.append({
            "N": n, "X": m["X_mean_rps"], "std": m["X_std_rps"], "step": k, "offered": m["offered_rps"],
            "metrics": m, "generator_limited": bool(reasons), "limited_reasons": reasons,
            "stop_reason": run.summary["stop_reason"], "steps_run": len(run.steps),
            "last_step_ratio": last["ratio"],
        })
    by_n = {p["N"]: p for p in points}

    # ---- повтори: основна серія + додаткові прогони тих самих N
    repeats = {p["N"]: [{"source": "основна серія", "X": p["X"], "std": p["std"], "step": p["step"]}] for p in points}
    series_runs = {"основна серія": {p["N"]: p["X"] for p in points}}
    for spec in a.extra:
        d, t, label = (spec.split("::") + ["", ""])[:3]
        d = d if os.path.isabs(d) else os.path.join(ROOT, d)
        label = label or (os.path.basename(os.path.normpath(d)) + (f" / {t}" if t else ""))
        mf = load_json(os.path.join(d, f"{t}manifest.json"))
        series_runs[label] = {}
        for n in sorted(int(k) for k in mf["runs"]):
            r = Run(d, t, n)
            k = r.x_step()
            m = r.step_metrics(k, limit_cpu, db)
            repeats.setdefault(n, []).append({"source": label, "X": m["X_mean_rps"], "std": m["X_std_rps"], "step": k})
            series_runs[label][n] = m["X_mean_rps"]
    use_rep = bool(a.extra)
    for p in points:
        xs = [r["X"] for r in repeats[p["N"]]]
        p["X_runs"] = repeats[p["N"]]
        p["X_median"], p["X_min"], p["X_max"] = statistics.median(xs), min(xs), max(xs)
        p["X_fit"] = p["X_median"] if use_rep else p["X"]

    # ---- крок 10: C(N), σ, κ, N*
    usl_res = {"G": g, "basis": "медіана за всіма прогонами" if use_rep else "основна серія"}
    fit_pts = [(p["N"], p["X_fit"]) for p in points if p["N"] > 1 and not p["generator_limited"]]
    one = by_n.get(1)
    if one is not None:
        one = dict(one, X=one["X_fit"])
    if one is None or one["generator_limited"]:
        usl_res["error"] = ("точка N = 1 обмежена генератором (або відсутня): зменште ліміт CPU контейнера web вдвічі "
                            "(WEB_CPUS у .env) і повторіть серію з іншим --tag")
    elif len(fit_pts) < 2:
        usl_res["error"] = (f"для підбору лишилось {len(fit_pts)} точок з N > 1 (потрібно щонайменше 2): "
                            "зменште ліміт CPU контейнера web вдвічі і повторіть серію з іншим --tag")
    else:
        usl_res.update(fit_usl(one["X"], fit_pts))
        usl_res["points_used"] = [n for n, _ in fit_pts]
    for p in points:  # C(N) для таблиці
        p["C"] = p["X_fit"] / one["X"] if one else None
    # чутливість підбору до вибору серії: окремо кожна серія, де є N = 1 і щонайменше дві точки з N > 1
    series_fits = []
    for label, xs in series_runs.items():
        if 1 in xs and len([n for n in xs if n > 1]) >= 2:
            try:
                ft = fit_usl(xs[1], [(n, x) for n, x in xs.items() if n > 1])
                series_fits.append({"label": label, "X": {str(n): x for n, x in sorted(xs.items())},
                                    "sigma": ft["sigma"], "kappa": ft["kappa"], "N_star": ft["N_star"], "method": ft["method"]})
            except ValueError as e:
                series_fits.append({"label": label, "error": str(e)})

    # ---- крок 9: сходинки прогону на одному екземплярі
    step9 = []
    if 1 in runs:
        r1 = runs[1]
        for k in range(1, len(r1.steps) + 1):
            m = r1.step_metrics(k, limit_cpu, db)
            step9.append({"step": k, "offered": m["offered_rps"], "achieved": m["X_mean_rps"],
                          "p99_client_ms": m["p99_client_ms"], "server_p99": m["server_p99"]})

    # ---- крок 11: ресурси на сходинці X(N_max), М2
    n_max = max(ns)
    pmax = by_n[n_max]
    rmax = runs[n_max]
    w0, w1 = rmax.window(pmax["step"])
    stats = docker_stats_window(os.path.join(res, f"{rmax.name}_docker_stats.jsonl"), w0, w1)
    m2_series = {}
    for n in sorted({ns[0], n_max}):
        run = runs[n]
        ser = []
        for k in range(1, len(run.steps) + 1):
            m = run.step_metrics(k, limit_cpu, db)
            ser.append({"step": k, "offered_rps": m["offered_rps"], "active": m["pg_states"].get("active", 0.0),
                        "idle": m["pg_states"].get("idle", 0.0), "idle_in_transaction": m["pg_states"].get("idle in transaction", 0.0),
                        "tps": m["pg_tps"], "numbackends": m["pg_numbackends"]})
        m2_series[n] = ser

    analysis = {
        "tag": tag,
        "environment": {"docker": manifest.get("docker"), "web_limits": limits, "G_rps": g, "procs": manifest.get("procs"),
                        "noop": manifest.get("noop")},
        "points": points, "usl": usl_res, "step9": step9, "series_fits": series_fits,
        "host": {str(n): host_stats(os.path.join(res, f"{runs[n].name}_docker_stats.jsonl")) for n in ns},
        "step11": {"N": n_max, "step": pmax["step"], "docker_stats": stats, "m2_at_x": m2_series[n_max][pmax["step"] - 1]},
        "m2_series": {str(k): v for k, v in m2_series.items()},
    }

    # ---- діаграми
    figures = []
    if not a.no_figures:
        plt = setup_matplotlib()
        fdir = os.path.join(res, "figures")
        os.makedirs(fdir, exist_ok=True)
        for n in sorted({ns[0], n_max}):
            path = os.path.join(fdir, f"{tag}step8_n{n}.png")
            figure_step8(plt, runs[n], path, limit_cpu, pool)
            figures.append(path)
        path = os.path.join(fdir, f"{tag}usl.png")
        figure_usl(plt, path, one["X"] if one else points[0]["X_fit"],
                   [{"N": p["N"], "X": p["X_fit"], "std": p["std"], "lo": p["X_min"], "hi": p["X_max"],
                     "runs": [r["X"] for r in p["X_runs"]], "generator_limited": p["generator_limited"]} for p in points],
                   usl_res if "sigma" in usl_res else None, g, n_max, use_rep)
        figures.append(path)
        path = os.path.join(fdir, f"{tag}m2.png")
        figure_m2(plt, path, m2_series, pool)
        figures.append(path)
    analysis["figures"] = [os.path.relpath(p, ROOT) for p in figures]

    with open(os.path.join(res, f"{tag}analysis.json"), "w", encoding="utf-8") as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)
    with open(os.path.join(res, f"{tag}analysis.md"), "w", encoding="utf-8") as f:
        f.write(render_tables(analysis, pool, limit_cpu))
    print(f"{os.path.join(res, tag + 'analysis.json')}, {os.path.join(res, tag + 'analysis.md')}"
          + (f", {os.path.join(res, 'figures')}/*.png" if figures else ""))
    for p in points:
        flag = " (обмежено генератором)" if p["generator_limited"] else ""
        print(f"  N={p['N']}: X = {fmt(p['X'], 1)} ± {fmt(p['std'], 1)} rps, сходинка {p['step']}{flag}"
              + (f"; медіана за {len(p['X_runs'])} прогонами {fmt(p['X_median'], 1)} (від {fmt(p['X_min'], 1)} до {fmt(p['X_max'], 1)})"
                 if use_rep else ""))
    if "sigma" in usl_res:
        print(f"  σ = {fmt(usl_res['sigma'], 4)}, κ = {fmt(usl_res['kappa'], 5)}, N* = {fmt(usl_res['N_star'], 2)}")
    else:
        print("  УЗМ не підібрано:", usl_res["error"])


# ------------------------------------------------------------------ таблиці Markdown

def table(header, rows, text_first=False):
    aligns = ["---" if (i == 0 and text_first) else "---:" for i in range(len(header))]
    out = ["| " + " | ".join(header) + " |", "|" + "|".join(aligns) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def server_p99_text(q):
    if q is None:
        return "—", "—", "—"
    if q["beyond"]:
        return f"> {fmt(q['lower_ms'], 0)}", f"> {fmt(q['lower_ms'], 0)} мс", "не обмежена зверху"
    return (fmt(q["estimate_ms"], 1), f"({fmt(q['lower_ms'], 3)}; {fmt(q['upper_ms'], 3)}] мс", f"{fmt(q['max_error_ms'], 3)} мс")


def render_tables(an, pool, limit_cpu):
    out = []
    env = an["environment"]
    d = env.get("docker") or {}
    out.append("## Умови вимірювань\n")
    out.append(table(["Параметр", "Значення"], [
        ["CPU, виділені Docker", d.get("ncpu", "—")],
        ["Памʼять, виділена Docker, ГіБ", fmt(d.get("mem_total_bytes", 0) / 2**30, 2) if d.get("mem_total_bytes") else "—"],
        ["Ліміт контейнера web", f"{env['web_limits'].get('WEB_CPUS', '—')} CPU, {env['web_limits'].get('WEB_MEMORY', '—')}"],
        ["Розмір пулу зʼєднань", pool],
        ["Процесів генератора", env.get("procs", "—")],
        ["Межа генератора G, запитів/с", fmt(env["G_rps"], 1)],
    ], text_first=True))
    out.append("\n## Таблиця X(N) і показники сходинки, де досягнуто X(N)\n")
    rows = []
    for p in an["points"]:
        m = p["metrics"]
        sp, _, _ = server_p99_text(m["server_p99"])
        rows.append([
            p["N"], fmt(p["X"], 1), fmt(p["std"], 1), f"{p['step']} ({p['offered']} rps)", fmt(m["p99_client_ms"], 1), sp,
            fmt((m["non_2xx_share"] or 0) * 100, 2), fmt(m["cpu_cores_per_instance"], 3),
            fmt((m["cpu_share_of_limit"] or 0) * 100, 0), fmt(m["inflight_per_instance"], 1),
            "так" if p["generator_limited"] else "ні",
        ])
    out.append(table(["N", "X(N), rps", "розкид, rps", "сходинка", "p99 клієнта, мс", "p99 сервера, мс", "не 2xx, %",
                      "CPU на екз., ядер", "від ліміту, %", "запитів в обробці на екз.", "обмежено генератором"], rows))
    if any(len(p["X_runs"]) > 1 for p in an["points"]):
        out.append("\n## Усі прогони і медіана X(N)\n")
        rows = []
        for p in an["points"]:
            rows.append([p["N"], "; ".join(f"{fmt(r['X'], 1)} ({r['source']})" for r in p["X_runs"]), len(p["X_runs"]),
                         fmt(p["X_median"], 1), f"{fmt(p['X_min'], 1)}–{fmt(p['X_max'], 1)}"])
        out.append(table(["N", "прогони X(N), rps", "кількість", "медіана, rps", "мінімум–максимум"], rows))
    out.append("\n## Підбір універсального закону масштабованості\n")
    u = an["usl"]
    if "sigma" in u:
        out.append(table(["N", "X(N), rps", "C(N)", "y = N/C − 1", "X за законом, rps", "відхилення, %"], [
            [r["N"], fmt(r["X"], 1), fmt(r["C"], 3), fmt(r["y"], 4), fmt(r["X_pred"], 1), fmt(r["rel_err"] * 100, 1)]
            for r in u["points"]]))
        out.append(f"\nλ = X(1) = {fmt(u['lambda'], 1)} rps; σ = {fmt(u['sigma'], 4)}; κ = {fmt(u['kappa'], 5)}; "
                   f"метод: {u['method']}; у підборі N = {u['points_used']}.")
        if u["N_star"]:
            out.append(f"N* = √((1 − σ)/κ) = {fmt(u['N_star'], 2)}; X(N*) = {fmt(u['X_at_N_star'], 1)} rps.")
        elif u["X_limit"]:
            out.append(f"κ = 0: максимуму немає, X(N) наближається до λ/σ = {fmt(u['X_limit'], 1)} rps.")
        else:
            out.append("σ = κ = 0: масштабування лінійне, максимуму немає.")
    else:
        out.append(f"Підбір неможливий: {u['error']}.")
    if an.get("series_fits"):
        out.append("\n### Чутливість підбору до вибору серії\n")
        rows = []
        for f in an["series_fits"]:
            if "error" in f:
                rows.append([f["label"], "—", "—", "—", "—", f["error"]])
            else:
                rows.append([f["label"], ", ".join(f"{n}: {fmt(x, 0)}" for n, x in f["X"].items()),
                             fmt(f["sigma"], 4), fmt(f["kappa"], 5), fmt(f["N_star"], 2) if f["N_star"] else "—", f["method"]])
        out.append(table(["серія", "X(N), rps", "σ", "κ", "N*", "метод"], rows, text_first=True))
    out.append("\n## Крок 9: p99 клієнта і сервера на кожній сходинці прогону на одному екземплярі\n")
    rows = []
    for s in an["step9"]:
        est, bucket, err = server_p99_text(s["server_p99"])
        rows.append([s["step"], s["offered"], fmt(s["achieved"], 1), fmt(s["p99_client_ms"], 1), est, bucket, err])
    out.append(table(["сходинка", "задано, rps", "досягнуто, rps", "p99 клієнта, мс", "p99 сервера, мс",
                      "бакет", "найбільша похибка"], rows))
    s11 = an["step11"]
    out.append(f"\n## Крок 11: ресурси на сходинці {s11['step']}, де досягнуто X({s11['N']})\n")
    st = s11["docker_stats"]
    if st:
        rows = [[g, fmt(v["cpu_cores"], 2), fmt(v["mem_mib"], 0)] for g, v in sorted(st["groups"].items())]
        out.append(table(["контейнер (середнє на один)", "CPU, ядер", "памʼять, МіБ"], rows, text_first=True))
        if st.get("generator_cpu_cores") is not None:
            out.append(f"\nПроцеси генератора: {fmt(st['generator_cpu_cores'], 2)} ядер; процесор хоста: "
                       f"{fmt(st['host_cpu_percent'], 0)} %; памʼять хоста: {fmt(st['host_mem_percent'], 0)} %.")
        out.append(f"\nВибірок docker stats у вікні: {st['samples']}.")
    m2 = s11["m2_at_x"]
    out.append(f"\nМ2 на цій сходинці: зʼєднань active {fmt(m2['active'], 1)}, idle {fmt(m2['idle'], 1)} "
               f"(разом {fmt(m2['active'] + m2['idle'], 1)} при N · пул = {s11['N'] * pool}); "
               f"транзакцій за секунду {fmt(m2['tps'], 0)}.")
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    main()
