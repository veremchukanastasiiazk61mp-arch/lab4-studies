
import argparse
import json
import math
import os
import statistics
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from analyze import (GRID, INK, INK2, SERIES, SURFACE, counter_delta, fmt, interp,  # noqa: E402
                     load_json, load_seconds, setup_matplotlib)
from usl import fit_usl, quantile_from_buckets, usl  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PATH = "/studies/{study_id}"
POD_COLORS = SERIES + ["#8a5cd0"]   # п'ять подів: п'ять різних кольорів

# ЛР2 (Docker Compose, 0,5 CPU на екземпляр): X(N) за всіма прогонами, rps (таблиця 1а звіту ЛР2)
COMPOSE_RUNS = {1: [150.1, 295.1, 374.7, 198.4], 2: [623.5, 550.5], 3: [648.9, 623.6], 4: [706.5, 423.5]}


class Run:
    def __init__(self, res, tag, n):
        self.n, self.name = n, f"{tag}step_n{n}"
        self.summary = load_json(os.path.join(res, f"{self.name}.json"))
        self.rows = load_seconds(os.path.join(res, f"{self.name}_seconds.csv"))
        self.prom = load_json(os.path.join(res, f"{tag}prom_n{n}.json"))
        cfg = self.summary["config"]
        self.t0, self.warmup, self.step_s, self.transient = cfg["t0_unix"], cfg["warmup_s"], cfg["step_seconds"], cfg["transient_s"]
        off = self.prom["clock_offset_s"]   # хост − Prometheus: переводить мітки Prometheus у час хоста
        self.series = {k: [(s["metric"], [(t + off, v) for t, v in s["values"]]) for s in lst]
                       for k, lst in self.prom["series"].items()}
        self.host_path = os.path.join(res, f"{self.name}_host.jsonl")
        k = self.summary["X_step"]
        self.step = self.summary["steps"][k - 1]
        self.w0 = self.t0 + self.warmup + (k - 1) * self.step_s + self.transient   # вікно усереднення сходинки X
        self.w1 = self.t0 + self.warmup + k * self.step_s

    def pods(self):
        """Поди, що працювали у вікні сходинки X: за рядами up == 1."""
        out = []
        for m, vals in self.series["up"]:
            if any(self.w0 <= t <= self.w1 and v == 1 for t, v in vals):
                out.append(m["pod"])
        return sorted(set(out))

    def by_pod(self, key, **match):
        res = defaultdict(list)
        for m, vals in self.series[key]:
            if all(m.get(a) == b for a, b in match.items()):
                res[m["pod"]].append((m, vals))
        return res

    def pod_rate(self, pod, t0, t1):
        d = sum(counter_delta(v, t0, t1) for _, v in self.by_pod("requests", path=PATH).get(pod, []))
        return d / (t1 - t0)

    def pod_p99_ms(self, pods, t0, t1):
        """p99 сервера за приростами бакетів гістограми за [t0, t1], сума за переліком подів."""
        cum = defaultdict(float)
        for pod in pods:
            for m, vals in self.by_pod("bucket", path=PATH).get(pod, []):
                cum[m["le"]] += counter_delta(vals, t0, t1)
        if not cum.get("+Inf"):
            return None
        finite = sorted((float(le), c) for le, c in cum.items() if le != "+Inf")
        q = quantile_from_buckets([b for b, _ in finite], [c for _, c in finite], cum["+Inf"], 0.99)
        return None if q is None else {"ms": q["estimate"] * 1000, "beyond": q["beyond"], "max_err_ms": None if q["max_error"] is None else q["max_error"] * 1000}

    def throttling(self, pod, t0, t1):
        per = sum(counter_delta(v, t0, t1) for _, v in self.by_pod("cfs_periods").get(pod, []))
        thr = sum(counter_delta(v, t0, t1) for _, v in self.by_pod("cfs_throttled").get(pod, []))
        return thr / per if per > 0 else None

    def cpu_cores(self, pod, t0, t1):
        d = sum(counter_delta(v, t0, t1) for _, v in self.by_pod("cpu_usage").get(pod, []))
        return d / (t1 - t0)

    def host(self):
        cpu, mem = [], []
        try:
            for line in open(self.host_path, encoding="utf-8"):
                r = json.loads(line)
                cpu.append(r["host_cpu_percent"]); mem.append(r["host_mem_percent"])
        except OSError:
            return None
        return {"samples": len(cpu), "cpu_mean": statistics.mean(cpu), "cpu_max": max(cpu),
                "mem_mean": statistics.mean(mem), "mem_max": max(mem)} if cpu else None


def analyse_run(run):
    pods = run.pods()
    rates = {p: run.pod_rate(p, run.w0, run.w1) for p in pods}
    x_prom = sum(rates.values())
    avg = x_prom / len(pods) if pods else float("nan")
    hot = max(rates, key=rates.get) if rates else None
    cold = min(rates, key=rates.get) if rates else None
    thr = {p: run.throttling(p, run.w0, run.w1) for p in pods}
    thr_vals = [v for v in thr.values() if v is not None]
    out = {
        "N": run.n, "X": run.step["achieved_mean_rps"], "std": run.step["achieved_std_rps"], "step": run.summary["X_step"],
        "offered": run.step["offered_rps"], "p99_client_ms": run.step["p99_ms"],
        "p99_server": run.pod_p99_ms(pods, run.w0, run.w1),
        "pods": pods, "n_pods_found": len(pods), "rates": rates, "X_prom": x_prom,
        "I": (rates[hot] / avg) if hot and avg > 0 else None,
        "hot": hot, "cold": cold,
        "thr_mean": statistics.fmean(thr_vals) if thr_vals else None,
        "thr_max": max(thr_vals) if thr_vals else None,
        "thr": thr,
        "cpu": {p: run.cpu_cores(p, run.w0, run.w1) for p in pods},
        "host": run.host(), "stop_reason": run.summary["stop_reason"],
    }
    if run.n > 1 and hot:
        out["hot_p99"] = run.pod_p99_ms([hot], run.w0, run.w1)
        out["cold_p99"] = run.pod_p99_ms([cold], run.w0, run.w1)
        out["hot_thr"], out["cold_thr"] = thr[hot], thr[cold]
        out["hot_rate"], out["cold_rate"] = rates[hot], rates[cold]
    return out


def pod_rate_series(run, window=10, grid=5):
    """Інтенсивність кожного пода (запитів за секунду) у часі: приріст лічильника за ковзне вікно."""
    t_end = run.t0 + len(run.rows)
    ts = [run.t0 + window + i * grid for i in range(int((t_end - run.t0 - window) // grid) + 1)]
    out = {}
    for pod in run.by_pod("requests", path=PATH):
        out[pod] = [run.pod_rate(pod, t - window, t) for t in ts]
    return [t - run.t0 for t in ts], out


def figure_pods(plt, run, out_path):
    ts, rates = pod_rate_series(run)
    fig, ax = plt.subplots(figsize=(9, 5), constrained_layout=True)
    off = [r["offered_rps"] for r in run.rows]
    ach = [r["achieved_rps"] for r in run.rows]
    ax.plot(range(len(off)), off, color=INK2, linewidth=1.1, linestyle="--", label="задана")
    for i, (pod, vals) in enumerate(sorted(rates.items())):
        ax.plot(ts, vals, color=POD_COLORS[i % len(POD_COLORS)], label=pod[-5:])
    ax.axvspan(run.w0 - run.t0, run.w1 - run.t0, color=SERIES[0], alpha=0.10, linewidth=0)
    ax.annotate("сходинка X(N)", (run.w0 - run.t0, ax.get_ylim()[1]), xytext=(4, -12), textcoords="offset points", fontsize=8.5, color=INK2)
    ax.set_xlabel("час прогону, с")
    ax.set_ylabel("запитів за секунду на под")
    ax.set_title(f"Запити на кожен под, N = {run.n} (вікно 10 с); пунктир — задана інтенсивність усього сервісу")
    ax.legend(loc="upper left", ncol=3)
    ax.set_ylim(bottom=0)
    fig.savefig(out_path)
    plt.close(fig)


def figure_usl(plt, out_path, k8s_rows, k8s_fit, comp_rows, comp_fit):
    fig, ax = plt.subplots(figsize=(9, 5.2), constrained_layout=True)
    nmax = 5
    ns = [i / 10 for i in range(10, nmax * 10 + 1)]
    for rows, fit, color, name, dx in ((comp_rows, comp_fit, SERIES[2], "Compose (ЛР2)", -0.05), (k8s_rows, k8s_fit, SERIES[0], "Kubernetes", 0.05)):
        x1 = rows[0]["X"]
        ax.plot(ns, [usl(n, x1, fit["sigma"], fit["kappa"]) / x1 for n in ns], color=color, linewidth=1.5,
                label=f"{name}: σ = {fmt(fit['sigma'], 3)}, κ = {fmt(fit['kappa'], 4)}")
        ax.errorbar([r["N"] + dx for r in rows], [r["X"] / x1 for r in rows],
                    yerr=[[(r["X"] - r["lo"]) / x1 for r in rows], [(r["hi"] - r["X"]) / x1 for r in rows]],
                    fmt="o", color=color, ecolor=color, markersize=7, capsize=4, markeredgecolor=SURFACE)
    ax.plot(ns, ns, color=INK2, linewidth=1, linestyle=":", label="лінійне C(N) = N")
    ax.set_xlabel("кількість екземплярів N")
    ax.set_ylabel("C(N) = X(N) / X(1)")
    ax.set_title("Відносна пропускна здатність: Compose і Kubernetes")
    ax.set_xticks(range(1, nmax + 1))
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper left")
    fig.savefig(out_path)
    plt.close(fig)


def md_table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---:" if i else "---" for i, _ in enumerate(header)) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def p99_text(q):
    if not q:
        return "—"
    if q["beyond"]:
        return "> 1024"
    return fmt(q["ms"], 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="")
    ap.add_argument("--ns", type=int, nargs="+", default=[1, 2, 3, 4, 5])
    a = ap.parse_args()
    res = os.path.join(ROOT, "results")
    figs = os.path.join(ROOT, "report", "lab3")
    os.makedirs(figs, exist_ok=True)

    runs = {n: Run(res, a.tag, n) for n in a.ns if os.path.exists(os.path.join(res, f"{a.tag}prom_n{n}.json"))}
    an = {n: analyse_run(r) for n, r in runs.items()}
    md = []

    # ---- крок 7
    md.append("Таблиця 1. Результати сходинкових прогонів (сходинка, на якій досягнуто X(N))\n")
    md.append(md_table(
        ["N", "сходинка (задано, rps)", "X(N), rps", "розкид (σ за 20 с), rps", "p99 клієнта, мс", "p99 сервера, мс", "дроселювання CPU: середнє за подами", "дроселювання CPU: найбільше серед подів"],
        [[n, f"{an[n]['step']} ({an[n]['offered']})", fmt(an[n]["X"], 1), fmt(an[n]["std"], 1), fmt(an[n]["p99_client_ms"], 1),
          p99_text(an[n]["p99_server"]), fmt((an[n]["thr_mean"] or 0) * 100, 1) + " %", fmt((an[n]["thr_max"] or 0) * 100, 1) + " %"] for n in sorted(an)]))
    for n in sorted(an):
        if an[n]["n_pods_found"] != n:
            md.append(f"\nУВАГА: для N = {n} знайдено {an[n]['n_pods_found']} подів у вікні сходинки X.")

    # ---- крок 8
    md.append("\n\nТаблиця 2. Дисбаланс навантаження на сходинці X(N)\n")
    rows8 = []
    for n in sorted(an):
        if n == 1:
            continue
        r = an[n]
        rows8.append([n, fmt(r["X_prom"], 1), fmt(r["X_prom"] / n, 1), fmt(r["hot_rate"], 1), fmt(r["cold_rate"], 1), fmt(r["I"], 2),
                      p99_text(r["hot_p99"]), p99_text(r["cold_p99"]), fmt((r["hot_thr"] or 0) * 100, 1) + " %", fmt((r["cold_thr"] or 0) * 100, 1) + " %"])
    md.append(md_table(["N", "Σ за подами, rps", "X_avg, rps", "X_max, rps", "X_min, rps", "I = X_max / X_avg",
                        "p99 серв. найзавант., мс", "p99 серв. найменш., мс", "дросел. найзавант.", "дросел. найменш."], rows8))
    md.append("\n\nТаблиця 2а. Інтенсивність кожного пода на сходинці X(N)\n")
    rows8a = []
    for n in sorted(an):
        for pod, v in sorted(an[n]["rates"].items(), key=lambda kv: -kv[1]):
            rows8a.append([n, pod[-5:], fmt(v, 1), fmt((an[n]["thr"][pod] or 0) * 100, 1) + " %", fmt(an[n]["cpu"][pod], 2)])
    md.append(md_table(["N", "под", "запитів за секунду", "дроселювання CPU", "CPU, ядер"], rows8a))

    # ---- хост
    md.append("\n\nТаблиця 3. Завантаження комп'ютера (psutil) за весь час прогону\n")
    md.append(md_table(["N", "вибірок", "CPU середнє, %", "CPU максимум, %", "памʼять середня, %", "памʼять максимум, %"],
                       [[n, an[n]["host"]["samples"], fmt(an[n]["host"]["cpu_mean"], 0), fmt(an[n]["host"]["cpu_max"], 0), fmt(an[n]["host"]["mem_mean"], 0), fmt(an[n]["host"]["mem_max"], 0)]
                        for n in sorted(an) if an[n]["host"]]))

    # ---- крок 9
    k8s_rows = [{"N": n, "X": an[n]["X"], "lo": an[n]["X"] - an[n]["std"], "hi": an[n]["X"] + an[n]["std"]} for n in sorted(an)]
    fit = None
    if len(an) >= 3:
        fit = fit_usl(an[1]["X"], [(n, an[n]["X"]) for n in sorted(an) if n > 1])
    comp_meds = {n: statistics.median(v) for n, v in COMPOSE_RUNS.items()}
    comp_rows = [{"N": n, "X": comp_meds[n], "lo": min(v), "hi": max(v)} for n, v in sorted(COMPOSE_RUNS.items())]
    comp_fit = fit_usl(comp_meds[1], [(n, comp_meds[n]) for n in (2, 3, 4)])
    md.append("\n\nТаблиця 4. Підбір σ і κ для кластера (без вільного члена)\n")
    if fit:
        md.append(md_table(["N", "X(N), rps", "C(N)", "y = N/C − 1", "X за законом, rps", "відхилення, %"],
                           [[p["N"], fmt(p["X"], 1), fmt(p["C"], 3), fmt(p["y"], 4), fmt(p["X_pred"], 1), fmt(p["rel_err"] * 100, 1)] for p in fit["points"]]))
        md.append(f"\n\nλ = X(1) = {fmt(an[1]['X'], 1)} rps; σ = {fmt(fit['sigma'], 4)}, κ = {fmt(fit['kappa'], 5)}; "
                  f"N* = {fmt(fit['N_star'], 2) if fit.get('N_star') else '—'}; метод: {fit.get('method')} {fit.get('note', '')}")
        md.append(f"\nCompose (ЛР2, за медіанами): λ = {fmt(comp_meds[1], 1)}, σ = {fmt(comp_fit['sigma'], 4)}, κ = {fmt(comp_fit['kappa'], 5)}, N* = {fmt(comp_fit.get('N_star'), 2) if comp_fit.get('N_star') else '—'}")

    plt = setup_matplotlib()
    if a.ns and max(runs) >= 1:
        figure_pods(plt, runs[max(runs)], os.path.join(figs, f"pods_n{max(runs)}.png"))
    if fit:
        figure_usl(plt, os.path.join(figs, "usl_k8s_vs_compose.png"), k8s_rows, fit, comp_rows, comp_fit)

    def clean(o):
        if isinstance(o, dict):
            return {k: clean(v) for k, v in o.items()}
        if isinstance(o, (list, tuple)):
            return [clean(v) for v in o]
        return o
    with open(os.path.join(res, f"{a.tag}analysis_k8s.json"), "w", encoding="utf-8") as f:
        json.dump({"runs": clean(an), "fit": clean(fit), "compose_fit": clean(comp_fit)}, f, ensure_ascii=False, indent=1, default=str)
    with open(os.path.join(res, f"{a.tag}analysis_k8s.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main()
