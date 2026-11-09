
import argparse
import glob
import json
import os
import re
import statistics as st
from collections import defaultdict

RES = os.environ.get('RESULTS_DIR', os.path.join(os.path.dirname(__file__), '..', 'results'))


def load(pattern):
    out = []
    for p in sorted(glob.glob(os.path.join(RES, pattern))):
        with open(p) as f:
            out.append((os.path.basename(p), json.load(f)))
    return out


def med(runs, key):
    return st.median(r[key] for r in runs)


def group_open():
    g = defaultdict(list)
    for name, d in load("open_n*_r*_rep*.json"):
        m = re.match(r"open_n(\d+)_r(\d+)_rep(\d+)\.json", name)
        g[(int(m[1]), int(m[2]))].append(d)
    return g


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--concurrency", nargs=2, type=int, metavar=("INSTANCES", "RATE"))
    a = ap.parse_args()
    g = group_open()

    if a.concurrency:
        runs = g[tuple(a.concurrency)]
        print(max(1, round(st.median(r["little"]["L_measured"] for r in runs))))
        return

    L = []
    L.append("## Таблиця вимірювань (медіана з повторів)\n")
    L.append("| Інстансів | Задана, rps | Досягнута, rps | p50, мс | p95, мс | p99, мс | Не 2xx, % | Повторів |")
    L.append("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for (n, rate), runs in sorted(g.items()):
        L.append(
            f"| {n} | {rate} | {med(runs, 'achieved_rps'):.1f} "
            f"| {med(runs, 'p50_ms'):.1f} | {med(runs, 'p95_ms'):.1f} "
            f"| {med(runs, 'p99_ms'):.1f} "
            f"| {100 * med(runs, 'non2xx_share'):.2f} | {len(runs)} |"
        )
    L.append("\nДодатково (медіана): запізнення планувальника генератора p99, мс; середнє L у системі.\n")
    L.append("| Інстансів | Задана, rps | lag p99, мс | L (середнє у системі) |")
    L.append("|---:|---:|---:|---:|")
    for (n, rate), runs in sorted(g.items()):
        lag = st.median(r["generator_lag_p99_ms"] for r in runs)
        lm = st.median(r["little"]["L_measured"] for r in runs)
        L.append(f"| {n} | {rate} | {lag:.1f} | {lm:.2f} |")

    # --- 1. Прискорення і Карп-Флатт ---
    L.append("\n## Розрахунок 1. Прискорення і метрика Карпа-Флатта\n")
    L.append("S = X(2 інстанси) / X(1 інстанс) на тому самому рівні; e = (1/S − 1/N)/(1 − 1/N), N = 2.\n")
    L.append("| Рівень, rps | X1, rps | X2, rps | S | e | Коментар |")
    L.append("|---:|---:|---:|---:|---:|---|")
    rates = sorted({r for (_, r) in g})
    for rate in rates:
        if (1, rate) in g and (2, rate) in g:
            x1 = st.median(r["achieved_rps"] for r in g[(1, rate)])
            x2 = st.median(r["achieved_rps"] for r in g[(2, rate)])
            s = x2 / x1
            e = (1 / s - 1 / 2) / (1 - 1 / 2)
            sat = x1 < 0.95 * rate
            note = "1 інстанс не тримає рівень: S і e змістовні" if sat else "1 інстанс тримає рівень: S≈1 очікувано, e не змістовна"
            L.append(f"| {rate} | {x1:.1f} | {x2:.1f} | {s:.3f} | {e:.3f} | {note} |")

    # --- 2. Закон Літтла ---
    L.append("\n## Розрахунок 2. Перевірка закону Літтла (100 rps, 1 інстанс)\n")
    runs = g.get((1, 100), [])
    if runs:
        L.append("| Прогін | λ, rps | W, с | λ·W | L виміряне | Розбіжність, % |")
        L.append("|---:|---:|---:|---:|---:|---:|")
        diffs = []
        for i, r in enumerate(runs, 1):
            lt = r["little"]
            d = abs(lt["L_measured"] - lt["lambda_times_W"]) / lt["lambda_times_W"] * 100
            diffs.append(d)
            L.append(f"| {i} | {lt['lambda_rps']} | {lt['W_s']} | {lt['lambda_times_W']} | {lt['L_measured']} | {d:.2f} |")
        L.append(f"\nМедіана розбіжності: {st.median(diffs):.2f} %")

    # --- 3. Закритий контур ---
    L.append("\n## Розрахунок 3. Відкритий і закритий контур\n")
    closed = defaultdict(list)
    for name, d in load("closed_n*_r*_c*_rep*.json"):
        m = re.match(r"closed_n(\d+)_r(\d+)_c(\d+)_rep(\d+)\.json", name)
        closed[(int(m[1]), int(m[2]), int(m[3]))].append(d)
    for (n, rate, c), cruns in sorted(closed.items()):
        o = g.get((n, rate), [])
        if not o:
            continue
        L.append(f"Рівень {rate} rps, {n} інстанс(ів), конкурентність закритого контуру C = {c} "
                 f"(медіана L відкритого = {st.median(r['little']['L_measured'] for r in o):.1f}).\n")
        L.append("| Контур | Досягнута, rps | p95, мс | p50, мс | p99, мс |")
        L.append("|---|---:|---:|---:|---:|")
        L.append(f"| відкритий ({rate} rps задано) | {med(o, 'achieved_rps'):.1f} "
                 f"| {med(o, 'p95_ms'):.1f} | {med(o, 'p50_ms'):.1f} "
                 f"| {med(o, 'p99_ms'):.1f} |")
        L.append(f"| закритий (C = {c}) | {med(cruns, 'achieved_rps'):.1f} "
                 f"| {med(cruns, 'p95_ms'):.1f} | {med(cruns, 'p50_ms'):.1f} "
                 f"| {med(cruns, 'p99_ms'):.1f} |\n")

    # --- самоперевірка генератора ---
    L.append("\n## Самоперевірка генератора (ендпоінт /noop, відповідь без роботи)\n")
    L.append("| Задана, rps | Досягнута, rps | p99, мс | lag p99, мс |")
    L.append("|---:|---:|---:|---:|")
    for name, d in load("noop_*.json"):
        L.append(f"| {d['config']['rate']:.0f} | {d['achieved_rps']} | {d['p99_ms']} | {d['generator_lag_p99_ms']} |")

    # --- розподіл між інстансами ---
    L.append("\n## Розподіл запитів між інстансами (за заголовком X-Served-By)\n")
    for (n, rate), runs in sorted(g.items()):
        tot = defaultdict(int)
        for r in runs:
            for k, v in r["served_by"].items():
                tot[k] += v
        L.append(f"- {n} інстанс(ів), {rate} rps: {dict(tot)}")

    text = "\n".join(L) + "\n"
    with open(os.path.join(RES, "summary.md"), "w") as f:
        f.write(text)
    print(text)


if __name__ == "__main__":
    main()
