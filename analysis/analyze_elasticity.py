#!/usr/bin/env python3
"""ЛР3, кроки 10–12 і Д6: метрики еластичності прогонів із профілем стрибка.

Читає results/: step_nN.json (серія кроку 7, для X(N)), profile_fixed*, profile_hpa*, profile_hpa_init15*
(_seconds.csv, .json, _prom.json, _info.json). Пише results/analysis_elasticity.{json,md} і report/lab3/profile_*.png.
Запуск з кореня проєкту:  python analysis/analyze_elasticity.py
"""
import argparse
import json
import math
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(__file__))
from analyze import GRID, INK, INK2, SERIES, SURFACE, counter_delta, fmt, load_json, load_seconds, setup_matplotlib  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
R = 5
REQ_CPU = 0.5          # requests.cpu = 500m (варіант 5)
T_UP, T_DOWN = 30, 90  # початок і кінець стрибка в профілі, с
NAMES = {"profile_fixed": "без автомасштабування (R = 5)", "profile_hpa": "HPA", "profile_hpa_init15": "HPA, ініціалізація 15 с"}


class Prof:
    def __init__(self, res, name):
        self.name = name
        self.rows = load_seconds(os.path.join(res, f"{name}_seconds.csv"))
        self.summary = load_json(os.path.join(res, f"{name}.json"))
        self.info = load_json(os.path.join(res, f"{name}_info.json"))
        self.prom = load_json(os.path.join(res, f"{name}_prom.json"))
        self.t0 = self.summary["config"]["t0_unix"]
        profile = self.summary["config"]["profile"]
        self.T = sum(d for d, _ in profile)
        off = self.prom["clock_offset_s"]
        self.series = {k: [(s["metric"], [(t + off, v) for t, v in s["values"]]) for s in lst]
                       for k, lst in self.prom["series"].items()}
        self.rows = [r for r in self.rows if r["t_s"] < self.T]

    def step_fn(self, key):
        """Ряд-датчик (один ряд) як функція часу: значення останньої вибірки не пізніше t (стале між точками збору)."""
        vals = sorted((t, v) for _, vs in self.series[key] for t, v in vs)
        return vals

    @staticmethod
    def at(vals, t):
        last = vals[0][1] if vals else 0
        for ts, v in vals:
            if ts <= t:
                last = v
            else:
                break
        return last

    def s_of_t(self):
        vals = self.step_fn("replicas_ready")
        return [self.at(vals, self.t0 + t) for t in range(self.T)]

    def desired_of_t(self):
        vals = self.step_fn("hpa_desired")
        return [self.at(vals, self.t0 + t) if vals else None for t in range(self.T)]

    def cpu_rel(self, window=10):
        """Середнє за подами використання CPU відносно requests.cpu, %, у кожній секунді (вікно window с)."""
        out = []
        for t in range(self.T):
            a, b = self.t0 + t - window, self.t0 + t
            vals = []
            for _, vs in self.series["cpu_usage"]:
                if any(a - 6 <= ts <= b for ts, _ in vs) and vs[0][0] <= a:
                    vals.append(counter_delta(vs, a, b) / window / REQ_CPU * 100)
            out.append(statistics.fmean(vals) if vals else None)
        return out


def x_of_n(res):
    return {n: load_json(os.path.join(res, f"step_n{n}.json"))["X_rps"] for n in range(1, R + 1)
            if os.path.exists(os.path.join(res, f"step_n{n}.json"))}


def demand(z, xn):
    """d(t) = min{N ≤ R : X(N) ≥ z(t)}, інакше R."""
    for n in range(1, R + 1):
        if n in xn and xn[n] >= z:
            return n
    return R


def elasticity(p, xn):
    z = [r["offered_rps"] for r in p.rows]
    d = [demand(v, xn) for v in z]
    s = p.s_of_t()
    T = p.T
    m = {
        "T": T,
        "theta_U": sum(1 for i in range(T) if s[i] < d[i]) / T,
        "theta_O": sum(1 for i in range(T) if s[i] > d[i]) / T,
        "a_U": sum(max(d[i] - s[i], 0) for i in range(T)) / T,
        "a_O": sum(max(s[i] - d[i], 0) for i in range(T)) / T,
        "W": sum(s),
    }
    return z, d, s, m


def p99_second(p):
    return [r["p99_ms"] for r in p.rows]


def events(p, s, p99_jump_ref):
    """Часи подій кроку 11 (с від початку або кінця стрибка); None — не досягнуто в межах відрізка."""
    out = {}
    des = p.desired_of_t()
    ev = None
    if des and des[T_UP - 1] is not None:
        base = des[T_UP - 1]
        for t in range(T_UP, T_DOWN):
            if des[t] != base:
                ev = t - T_UP
                break
    out["first_desired_change"] = ev
    smax = max(s)
    tmax = next((t for t in range(T_UP, p.T) if s[t] == smax), None)
    out["max_ready"] = smax
    out["to_max_ready"] = (tmax - T_UP) if tmax is not None and tmax < T_DOWN else None
    p99 = p99_second(p)
    rec = None
    for t in range(T_UP, T_DOWN):
        win = p99[t:t + 10]
        if len(win) == 10 and all(v is not None and v < 1.2 * p99_jump_ref for v in win):
            rec = t - T_UP
            break
    out["p99_recovery"] = rec
    down = None
    for t in range(T_DOWN, p.T):
        if s[t] < s[t - 1]:      # будь-яке зменшення кількості готових реплік після кінця стрибка
            down = t - T_DOWN
            break
    out["first_scale_down"] = down
    return out


def new_pod_ready_times(p):
    """Час від створення пода до готовності для подів, створених під час прогону (Д6)."""
    created = {}
    for m, vs in p.series.get("pod_created", []):
        if vs:
            created[m["pod"]] = vs[-1][1]
    out = {}
    for m, vs in p.series["pod_ready"]:
        pod = m["pod"]
        if pod in created and created[pod] > p.t0 - 2:
            t_ready = next((t for t, v in vs if v == 1), None)
            if t_ready is not None:
                out[pod] = t_ready - created[pod]
    return out


def figure(plt, p, z, d, s, cpu, out_path, thr_ms, title):
    fig, axs = plt.subplots(2, 2, figsize=(11, 7), constrained_layout=True)
    t = list(range(p.T))
    ach = [r["achieved_rps"] for r in p.rows]
    ax = axs[0][0]
    ax.plot(t, z, color=INK2, linestyle="--", label="задана")
    ax.plot(t, ach, color=SERIES[0], label="досягнута")
    ax.set_ylim(0, 900); ax.set_ylabel("запитів за секунду"); ax.legend(loc="upper left")
    ax = axs[0][1]
    ax.plot(t, [v if v is not None else float("nan") for v in p99_second(p)], color=SERIES[1], label="p99 клієнта")
    ax.axhline(thr_ms, color=INK2, linewidth=1, linestyle=":", label="поріг 2·p99₀")
    ax.set_yscale("log"); ax.set_ylim(1, 10000); ax.set_ylabel("мс"); ax.legend(loc="upper left")
    ax = axs[1][0]
    ax.step(t, d, where="post", color=INK2, linestyle="--", label="d(t) потрібно")
    ax.step(t, s, where="post", color=SERIES[2], label="s(t) готових")
    des = p.desired_of_t()
    if any(v is not None for v in des):
        ax.step(t, [v if v is not None else float("nan") for v in des], where="post", color=SERIES[3], linewidth=1.2, label="бажана (HPA)")
    ax.set_ylim(0, 7.5); ax.set_yticks(range(0, 8)); ax.set_ylabel("реплік"); ax.legend(loc="upper right")
    ax = axs[1][1]
    ax.plot(t, [v if v is not None else float("nan") for v in cpu], color=SERIES[0], label="CPU / requests")
    ax.axhline(70, color=INK2, linewidth=1, linestyle=":", label="ціль HPA 70 %")
    ax.set_ylim(0, 250); ax.set_ylabel("% від requests.cpu"); ax.legend(loc="upper right")
    for ax in axs.flat:
        ax.axvline(T_UP, color=GRID, linewidth=1.4); ax.axvline(T_DOWN, color=GRID, linewidth=1.4)
        ax.set_xlim(0, p.T); ax.set_xlabel("час прогону, с")
    fig.suptitle(title, x=0.01, ha="left", fontsize=11)
    fig.savefig(out_path)
    plt.close(fig)


def cell(v, unit="с"):
    return "не досягнуто" if v is None else f"{fmt(v, 0)} {unit}"


def main():
    ap = argparse.ArgumentParser()
    a = ap.parse_args()
    res, figs = os.path.join(ROOT, "results"), os.path.join(ROOT, "report", "lab3")
    os.makedirs(figs, exist_ok=True)
    xn = x_of_n(res)
    names = [n for n in NAMES if os.path.exists(os.path.join(res, f"{n}_prom.json"))]
    profs = {n: Prof(res, n) for n in names}
    if "profile_fixed" not in profs:
        sys.exit("Немає results/profile_fixed_*: спершу виконайте прогін кроку 10")
    fixed = profs["profile_fixed"]
    seg = fixed.summary["steps"]
    p99_0 = seg[0]["p99_ms"]           # p99 клієнта на першому відрізку прогону без автомасштабування
    p99_jump = seg[1]["p99_ms"]        # p99 клієнта на рівні стрибка в тому ж прогоні
    thr = 2 * p99_0
    # Чутливість до викиду: p99₀ за ТЗ береться за весь перший відрізок (вікно 10–30 с) і чутливий до поодиноких
    # секунд-викидів; для порівняння рахуємо й за медіаною посекундних p99 першого відрізка (5–30 с).
    first = sorted(r["p99_ms"] for r in fixed.rows if 5 <= r["t_s"] < T_UP and r["p99_ms"] is not None)
    p99_0_med = statistics.median(first)
    thr_med = 2 * p99_0_med
    plt = setup_matplotlib()
    out = {"X_of_N": xn, "p99_0_median_ms": p99_0_med, "threshold_median_ms": thr_med, "p99_0_ms": p99_0, "p99_jump_fixed_ms": p99_jump, "threshold_ms": thr, "runs": {}}
    md = [f"Параметри: X(N) = {', '.join(f'{n}: {fmt(v, 1)}' for n, v in sorted(xn.items()))} rps; "
          f"p99₀ = {fmt(p99_0, 1)} мс (перший відрізок, без автомасштабування); поріг 2·p99₀ = {fmt(thr, 1)} мс; "
          f"p99 на рівні стрибка без автомасштабування = {fmt(p99_jump, 1)} мс; 1,2·p99 = {fmt(1.2 * p99_jump, 1)} мс. "
          f"Для порівняння: медіана посекундних p99 першого відрізка = {fmt(p99_0_med, 1)} мс, поріг за медіаною {fmt(thr_med, 1)} мс.\n"]
    rows12, rows11, rows_d6 = [], [], []
    for name, p in profs.items():
        z, d, s, m = elasticity(p, xn)
        cpu = p.cpu_rel()
        viol = sum(1 for v in p99_second(p) if v is not None and v > thr)
        viol_med = sum(1 for v in p99_second(p) if v is not None and v > thr_med)
        non2xx = int(sum(r["non_2xx"] for r in p.rows))
        conn = int(sum(r["conn_errors"] for r in p.rows))
        ev = events(p, s, p99_jump) if name != "profile_fixed" else None
        newp = new_pod_ready_times(p) if name != "profile_fixed" else {}
        out["runs"][name] = {"metrics": m, "violation_s": viol, "violation_s_median_threshold": viol_med, "non_2xx": non2xx, "conn_errors": conn, "events": ev,
                             "new_pod_ready_s": newp, "d_unique": sorted(set(d)), "s_max": max(s),
                             "s_changes": [[t, s[t]] for t in range(p.T) if t == 0 or s[t] != s[t - 1]],
                             "desired_changes": [[t, p.desired_of_t()[t]] for t in range(p.T) if p.desired_of_t()[t] is not None and (t == 0 or p.desired_of_t()[t] != p.desired_of_t()[t - 1])] if name != "profile_fixed" else []}
        rows12.append([NAMES[name], fmt(m["theta_U"], 3), fmt(m["theta_O"], 3), fmt(m["a_U"], 3), fmt(m["a_O"], 3),
                       fmt(m["W"], 0), viol, viol_med, non2xx, conn])
        if ev:
            rows11.append([NAMES[name], cell(ev["first_desired_change"]), cell(ev["to_max_ready"]) + f" (до {ev['max_ready']:.0f})" if ev["to_max_ready"] is not None else "не досягнуто",
                           cell(ev["p99_recovery"]), cell(ev["first_scale_down"])])
        if newp:
            rows_d6.append([NAMES[name], len(newp), fmt(statistics.fmean(newp.values()), 1), fmt(max(newp.values()), 1)])
        figure(plt, p, z, d, s, cpu, os.path.join(figs, f"{name}.png"), thr, f"Прогін: {NAMES[name]}")

    def table(h, rows):
        o = ["| " + " | ".join(h) + " |", "|" + "|".join("---:" if i else "---" for i, _ in enumerate(h)) + "|"]
        return "\n".join(o + ["| " + " | ".join(str(c) for c in r) + " |" for r in rows])
    md.append("Таблиця 5. Метрики еластичності\n")
    md.append(table(["прогін", "θ_U", "θ_O", "a_U", "a_O", "W, репліко-с", "секунд порушення порогу", "секунд порушення (поріг за медіаною)", "відповідей не 2xx", "помилок зʼєднання"], rows12))
    if rows11:
        md.append("\n\nТаблиця 6. Реакція HPA на стрибок (відлік від початку стрибка; останній стовпець — від його кінця)\n")
        md.append(table(["прогін", "до першої зміни бажаної кількості", "до готовності найбільшої кількості", "до p99 < 1,2·p99 стрибка (10 с поспіль)", "після кінця стрибка до першого зменшення"], rows11))
    if rows_d6:
        md.append("\n\nТаблиця 7. Час від створення нового пода до готовності (Д6)\n")
        md.append(table(["прогін", "нових подів", "середній, с", "найбільший, с"], rows_d6))
    with open(os.path.join(res, "analysis_elasticity.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1, default=str)
    with open(os.path.join(res, "analysis_elasticity.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main()
