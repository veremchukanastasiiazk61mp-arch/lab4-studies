#!/usr/bin/env python3
"""ЛР4: таблиці і діаграми за результатами scripts/lab4.py.

Читає results/lab4/*.json. Пише:
  results/lab4/analysis_lab4.json   усі обчислені величини
  results/lab4/analysis_lab4.md     таблиці для звіту
  report/lab4/*.png                 діаграми кроків 6, 7, 9
Запуск з кореня проєкту:  python analysis/analyze_lab4.py
Потрібно: pip install matplotlib
"""
import json
import math
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(__file__))
from analyze import GRID, INK, INK2, SERIES, fmt, setup_matplotlib  # noqa: E402
from usl import fit_usl, usl  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RES = os.path.join(ROOT, "results", "lab4")
FIG = os.path.join(ROOT, "report", "lab4")
POLL_MS, LEASE_S, R, F = 10, 5.0, 5, 0.15


def load(name):
    p = os.path.join(RES, name + ".json")
    if not os.path.exists(p):
        return None
    raw = open(p, "rb").read()
    try:
        return json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError:  # файли, записані на Windows у cp1251
        return json.loads(raw.decode("cp1251"))


def tab(head, rows):
    out = ["| " + " | ".join(head) + " |", "|" + "|".join(["---"] + ["---:"] * (len(head) - 1)) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def ms(x, d=1):
    return "—" if x is None else fmt(x * 1000, d)


def pct(x, d=1):
    return "—" if x is None else fmt(100 * x, d) + " %"


def main():
    os.makedirs(FIG, exist_ok=True)
    out, md = {}, []
    w = md.append

    # ---------------------------------------------------------------- крок 2
    s2 = load("step2")
    t_io = t_cpu = None
    if s2:
        t_io, t_cpu = s2["t_io_ms"], s2["t_cpu_ms"]
        out["step2"] = {k: s2[k] for k in ("t_io_ms", "t_cpu_ms", "done")}
        w("## Крок 2. Середні тривалості кроків задачі (20 задач, один воркер)\n")
        w(tab(["крок", "кількість", "середня тривалість, мс"],
              [[st, int(s2[st]["count"]), fmt(s2[st]["mean_ms"], 2)] for st in ("claim", "fetch", "process", "store")]))
        w(f"\nt_io = fetch + store = {fmt(t_io, 2)} мс, t_cpu = process = {fmt(t_cpu, 2)} мс; виконано {s2['done']} із 20.\n")

    # ---------------------------------------------------------------- кроки 3-4
    series = [("step3_naive_poll10", "наївне, T_poll = 10 мс"), ("step3_naive_poll1", "наївне, T_poll = 1 мс"),
              ("step4_skip_locked_poll1", "FOR UPDATE SKIP LOCKED, 1 мс"), ("step4_for_update_poll1", "FOR UPDATE без SKIP LOCKED, 1 мс")]
    rows, d_out = [], {}
    for key, label in series:
        r = load(key)
        if not r:
            continue
        d_out[key] = {k: r[k] for k in ("D_results", "D_logs", "n_dup_results", "n_dup_logs", "tasks", "results_rows",
                                        "claim_p99_s", "empty_claims", "task_claims", "max_lock_waits", "duration_s", "failed")}
        rows.append([label, r["tasks"], r["results_rows"], r["n_dup_results"], fmt(r["D_results"], 3), r["n_dup_logs"], fmt(r["D_logs"], 3),
                     ms(r["claim_p99_s"], 0), int(r["empty_claims"]), r["max_lock_waits"], fmt(r["duration_s"], 1)])
    if rows:
        out["steps34"] = d_out
        w("## Кроки 3 і 4. Дублювання і блокування\n")
        w(tab(["серія", "задач", "рядків у task_results", "n_dup (таблиця)", "D (таблиця)", "n_dup (журнали)", "D (журнали)",
               "p99 захоплення, мс (верхня межа бакета)", "порожніх захоплень", "найбільше очікувань Lock", "тривалість, с"], rows))
        w("")

    # ---------------------------------------------------------------- крок 5
    s5 = load("step5")
    if s5:
        out["step5"] = s5
        w("## Крок 5. Lease, heartbeat і fencing token (2 воркери, 100 задач)\n")
        zl = s5["zero_rows_log_victim"]
        w(tab(["величина", "значення"],
              [["зупинений воркер", s5["victim"]], ["задача в обробці на момент зупинки", s5["task_id"]],
               ["attempts / token до зупинки", f"{s5['before']['attempts']} / {s5['before']['token']}"],
               ["стан під час зупинки (status, attempts, token, worker)", s5["during_stop"].replace("|", ", ")],
               ["стан після відновлення (status, attempts, token, worker)", s5["after"].replace("|", ", ")],
               ["тривалість зупинки, с", fmt(s5["stopped_s"], 1)],
               ["рядків у task_results для цієї задачі", s5["results_for_task"]],
               ["рядків журналу зупиненого воркера про нуль оновлених рядків", len(zl)],
               ["задач із attempts > 1 за прогін", s5["attempts_gt1"]], ["задач у failed", s5["failed"]],
               ["задач з більше ніж одним результатом", s5["n_dup_results_all_tasks"]]]))
        w("\nРядки журналу зупиненого воркера:\n")
        for line in zl:
            w("    " + line.strip())
        w("\nРядки журналу воркерів про повернення задачі в чергу:\n")
        for line in s5["requeue_log_other"]:
            w("    " + line.strip())
        pr = s5.get("pid1_signal_probe")
        if pr:
            w(f"\nПеревірка команди із завдання: `{pr.get('command')}`; стан процесу до: {pr.get('state_before')}, після: "
              f"{pr.get('state_after')}; діє: {'так' if pr.get('works') else 'ні'}.\n")

    # ---------------------------------------------------------------- крок 6
    runs = {}
    for wk in (1, 2, R):
        r = load(f"step6_w{wk}")
        if r:
            runs[wk] = r
    fit = None
    if runs:
        w("## Крок 6. Пропускна здатність пулу X(W)\n")
        rows = []
        for wk, r in sorted(runs.items()):
            rows.append([wk, r["tasks"], fmt(r["X"], 2), fmt(r["std"], 2), fmt(r["window_s"], 0), ms(r["claim_p99_s"], 0),
                         fmt(r["xact_per_s"], 0), fmt(r["db_cpu_cores"], 2) if r["db_cpu_cores"] is not None else "—",
                         int(r["empty_claims"])])
        w(tab(["W", "задач у черзі", "X(W), задач/с", "розкид (σ посекундних), задач/с", "вікно, с", "p99 захоплення, мс",
               "транзакцій БД за секунду", "CPU пода бази, ядер", "порожніх захоплень"], rows))
        out["step6"] = {str(k): {kk: v[kk] for kk in ("X", "std", "claim_p99_s", "xact_per_s", "db_cpu_cores", "window_s", "tasks")}
                        for k, v in runs.items()}
        if 1 in runs and t_io is not None:
            x1 = runs[1]["X"]
            pred = 1 / ((t_io + t_cpu) / 1000 + POLL_MS / 2000)
            w(f"\nX(1) = {fmt(x1, 2)} задач/с; оцінка 1 / (t_io + t_cpu + T_poll / 2) = 1 / ({fmt(t_io, 2)} + {fmt(t_cpu, 2)} + "
              f"{fmt(POLL_MS / 2, 1)}) мс = {fmt(pred, 2)} задач/с; розбіжність {fmt(100 * (x1 - pred) / pred, 1)} %.\n")
            out["step6"]["x1_pred"] = pred
            out["step6"]["x1_vs_pred_pct"] = 100 * (x1 - pred) / pred
        if 1 in runs and len(runs) >= 3:
            x1 = runs[1]["X"]
            pts = [(wk, runs[wk]["X"]) for wk in sorted(runs) if wk > 1]
            fit = fit_usl(x1, pts)
            out["step6"]["fit"] = fit
            w(tab(["W", "X(W), задач/с", "C(W) = X(W)/X(1)", "y = W/C − 1", "X за законом, задач/с", "відхилення, %"],
                  [[1, fmt(x1, 2), "1,000", "0", fmt(x1, 2), "0"]] +
                  [[p["N"], fmt(p["X"], 2), fmt(p["C"], 3), fmt(p["y"], 4), fmt(p["X_pred"], 2), fmt(100 * p["rel_err"], 1)]
                   for p in fit["points"]]))
            w(f"\nλ = X(1) = {fmt(x1, 2)}; σ = {fmt(fit['sigma'], 4)}; κ = {fmt(fit['kappa'], 5)}; метод: {fit['method']}; "
              f"W* = {fmt(fit['N_star'], 2) if fit['N_star'] else '—'}; X(W*) = {fmt(fit['X_at_N_star'], 2) if fit['X_at_N_star'] else '—'}. "
              f"Точок із W > 1 лише {fit['n_points']}: два параметри підібрано за двома точками, тому закон проходить через них точно, "
              "і відхилення відсутні за побудовою; оцінки грубі.\n")
        try:
            plot_step6(runs, fit)
        except Exception as exc:  # noqa: BLE001
            print("рисунок step6 не побудовано:", exc)

    # ---------------------------------------------------------------- крок 7
    st, ov = load("step7_steady"), load("step7_overload")
    if st:
        w("## Крок 7. Закон Літтла\n")
        t_from, t_to = st["window"]
        gauge = [g["tasks_queued"] for g in st["gauge"] if t_from <= g["t"] <= t_to and g["tasks_queued"] is not None]
        samp = [x["queued"] for x in st["samples"] if "queued" in x and t_from <= x["t"] <= t_to]
        lq_g = statistics.fmean(gauge) if gauge else None
        lq_s = statistics.fmean(samp) if samp else None
        lam, wq = st["lambda"], st["W_q"]
        out["step7_steady"] = {"lambda": lam, "W_q_s": wq, "lambda_Wq": lam * wq, "Lq_gauge": lq_g, "Lq_db_samples": lq_s,
                               "diff_gauge_pct": 100 * (lq_g - lam * wq) / (lam * wq) if lq_g is not None and wq else None,
                               "diff_samples_pct": 100 * (lq_s - lam * wq) / (lam * wq) if lq_s is not None and wq else None,
                               "rate_target": st["rate"], "n_window": st["n_created_in_window"]}
        w(f"Сталий режим: інтенсивність створення {fmt(st['rate'], 2)} = 0,7·X(R) задач/с, 180 с, вікно останніх 120 с "
          f"({st['n_created_in_window']} задач).\n")
        w(tab(["величина", "значення"],
              [["λ, задач/с", fmt(lam, 2)], ["W_q, мс", fmt(wq * 1000, 2)], ["λ · W_q", fmt(lam * wq, 3)],
               ["L_q за вимірювачем tasks_queued (середнє, вибірка раз на 2 с)", fmt(lq_g, 3)],
               ["розбіжність, %", fmt(out["step7_steady"]["diff_gauge_pct"], 1)],
               ["L_q за вибіркою бази раз на секунду (для перевірки)", fmt(lq_s, 3)],
               ["розбіжність, %", fmt(out["step7_steady"]["diff_samples_pct"], 1)]]))
        w("\nУ сталому режимі черга майже порожня (її середня довжина менша за одну задачу), тому відносна розбіжність "
          "великі за малих абсолютних значень: вимірювач `tasks_queued` оновлюється раз на 5 с і пропускає короткі сплески.\n")
    if ov:
        out["step7_overload"] = {k: ov[k] for k in ("rate", "queue_at_end", "Wq_first100_s", "Wq_last100_s")}
        w(f"Перевантаження: інтенсивність {fmt(ov['rate'], 2)} = 1,3·X(R) задач/с, 120 с. Довжина черги на кінець генерації "
          f"{ov['queue_at_end']}; W_q перших 100 задач {fmt(ov['Wq_first100_s'] * 1000, 1)} мс, останніх 100 задач "
          f"{fmt(ov['Wq_last100_s'], 2)} с. Перевантаження: ріст W_q підтверджує, що закон Літтла для нього не діє, бо режим не сталий.\n")
    if st or ov:
        try:
            plot_step7(st, ov)
        except Exception as exc:  # noqa: BLE001
            print("рисунок step7 не побудовано:", exc)

    # ---------------------------------------------------------------- крок 8
    s8 = load("step8")
    if s8:
        out["step8"] = {k: s8[k] for k in ("exit_codes_from_pod_status", "exit_code_0_in_logs", "returned_to_queue_on_shutdown",
                                           "failed", "tasks", "n_dup_results", "attempts_gt1", "drained")}
        w("## Крок 8. Коректна зупинка за SIGTERM\n")
        codes = s8["exit_codes_from_pod_status"]
        w(tab(["величина", "значення"],
              [["коди завершення контейнерів (kubectl get pods -w)",
                ", ".join(f"{k}: {v['exitCode']} ({v['reason']})" for k, v in sorted(codes.items())) or "не зафіксовано"],
               ["рядків журналу «[exit_code_0]»", s8["exit_code_0_in_logs"]],
               ["задач, повернутих у чергу воркерами під час зупинки", len(s8["returned_to_queue_on_shutdown"])],
               ["задач у failed", s8["failed"]], ["задач з більше ніж одним результатом", s8["n_dup_results"]],
               ["задач з attempts > 1", s8["attempts_gt1"]], ["усього задач", s8["tasks"]]]))
        w("")

    # ---------------------------------------------------------------- крок 9
    f_runs = [(k, load(k)) for k in ("step9_fail015", "step9_f5_nohb", "step9_f5_hb")]
    f_runs = [(k, r) for k, r in f_runs if r]
    if f_runs:
        w("## Крок 9. FAIL_RATE = 0,15 і сценарій Ф5\n")
        t_cpu_ms = t_cpu
        names = {"step9_fail015": "A: FAIL_RATE = 0,15, L = 5 с, heartbeat",
                 "step9_f5_nohb": "B: Ф5, L = t_cpu/2 без heartbeat",
                 "step9_f5_hb": "C: Ф5, L = t_cpu/2 з heartbeat"}
        rows, o9 = [], {}
        xr = runs[R]["X"] if R in runs else None
        for key, r in f_runs:
            n = r["tasks"]
            sn = r["snapshot_at_generation_end"]
            dist = r["attempts_distribution_terminal"]
            term = max(sn["terminal"], 1)
            mean_att = sum(int(a) * c for a, c in dist.items()) / term
            o9[key] = {"restarts_total": r["restarts_total"], "tasks": n, "terminal_at_gen_end": sn["terminal"],
                       "queued_at_gen_end": sn["queued"], "attempts_gt1": sn["attempts_gt1_among_terminal"],
                       "share_gt1": sn["attempts_gt1_among_terminal"] / term, "failed_at_gen_end": sn["failed"],
                       "failed_final": r["failed_final"], "dup": r["n_dup_results"], "X_run": r["X_run"],
                       "mean_attempts": mean_att, "attempts_distribution": dist,
                       "zero_rows": r["zero_rows_log_lines"], "rate": r["rate"], "lease_s": r["lease_s"],
                       "heartbeat": r["heartbeat"], "unreadable": r.get("unreadable_log_pods")}
            rows.append([names[key], fmt(r["lease_s"], 3), "так" if r["heartbeat"] else "ні", r["restarts_total"], sn["terminal"],
                         sn["queued"], sn["attempts_gt1_among_terminal"], pct(sn["attempts_gt1_among_terminal"] / term),
                         sn["failed"], r["failed_final"], r["zero_rows_log_lines"], r["n_dup_results"], fmt(r["X_run"], 2),
                         fmt(mean_att, 3)])
        out["step9"] = o9
        w(tab(["прогін", "L, с", "heartbeat", "перезапусків контейнерів", "завершено за час генерації", "у черзі на кінець генерації",
               "attempts > 1", "частка", "failed на кінець генерації", "failed після спорожнення", "рядків «0 rows updated»",
               "задач з >1 результатом", "X у прогоні, задач/с", "середнє attempts"], rows))
        w(f"\nІнтенсивність створення 0,7·X(R) = {fmt(f_runs[0][1]['rate'], 2)} задач/с, генерація 300 с. "
          f"Очікування за FAIL_RATE f = {F}: частка задач з attempts > 1 дорівнює f = {pct(F)}, частка failed f³ = {pct(F ** 3, 3)}, "
          f"середня кількість захоплень 1 / (1 − f) = {fmt(1 / (1 - F), 3)}. X(R) без падінь = {fmt(xr, 2) if xr else '—'} задач/с.\n")
        w("Розподіл завершених за час генерації задач за attempts:\n")
        w(tab(["прогін"] + ["attempts = " + str(i) for i in (1, 2, 3, 4)],
              [[names[k]] + [o9[k]["attempts_distribution"].get(str(i), o9[k]["attempts_distribution"].get(i, 0)) for i in (1, 2, 3, 4)]
               for k, _ in f_runs]))
        try:
            plot_step9(f_runs)
        except Exception as exc:  # noqa: BLE001
            print("рисунок step9 не побудовано:", exc)

    # ---------------------------------------------------------------- Д2
    d2 = load("d2")
    if d2:
        out["d2"] = d2
        w("## Д2. Поле run_after\n")
        w(tab(["величина", "значення"],
              [["задач", d2["n"]], ["p50 (started_at − run_after), мс", fmt(d2["p50_s"] * 1000, 1)],
               ["p99 (started_at − run_after), мс", fmt(d2["p99_s"] * 1000, 1)],
               ["найменше, мс", fmt(d2["min_s"] * 1000, 1)], ["найбільше, мс", fmt(d2["max_s"] * 1000, 1)],
               ["задач, розпочатих раніше за run_after", d2["started_before_run_after"]],
               ["діапазон затримок run_after − created_at, с", d2["delay_range_s"].replace("|", " … ")]]))
        w("")

    with open(os.path.join(RES, "analysis_lab4.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1, ensure_ascii=False, default=str)
    with open(os.path.join(RES, "analysis_lab4.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md) + "\n")
    print("\n".join(md))
    print(f"\nзаписано {os.path.relpath(os.path.join(RES, 'analysis_lab4.md'), ROOT)} і рисунки в report/lab4/")


# ------------------------------------------------------------------ діаграми

def plot_step6(runs, fit):
    plt = setup_matplotlib()
    fig, ax = plt.subplots(figsize=(6.4, 4.0))
    ws = sorted(runs)
    xs = [runs[k]["X"] for k in ws]
    sd = [runs[k]["std"] for k in ws]
    ax.errorbar(ws, xs, yerr=sd, fmt="o", color=SERIES[0], capsize=4, label="X(W) ± розкид", zorder=3)
    if fit and 1 in runs:
        grid = [1 + i * (max(ws) - 1) / 100 for i in range(101)]
        ax.plot(grid, [usl(n, runs[1]["X"], fit["sigma"], fit["kappa"]) for n in grid], color=SERIES[1],
                label=f"закон: σ = {fmt(fit['sigma'], 3)}, κ = {fmt(fit['kappa'], 4)}")
    ax.plot(ws, [runs[1]["X"] * k for k in ws], color=INK2, ls="--", lw=1.0, label="лінійне масштабування") if 1 in runs else None
    ax.set_xticks(ws)
    ax.set_xlabel("кількість воркерів W")
    ax.set_ylabel("X(W), задач за секунду")
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.set_title("Пропускна здатність пулу воркерів")
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "x_of_w.png"))
    plt.close(fig)


def plot_step7(st, ov):
    plt = setup_matplotlib()
    panels = [(p, t) for p, t in ((st, "0,7 · X(R): сталий режим"), (ov, "1,3 · X(R): перевантаження")) if p]
    fig, axes = plt.subplots(1, len(panels), figsize=(6.2 * len(panels), 3.8), squeeze=False)
    for ax, (p, title) in zip(axes[0], panels):
        t0 = p["t0"]
        g = [(x["t"] - t0, x["tasks_queued"]) for x in p["gauge"] if x["tasks_queued"] is not None]
        if g:
            ax.step([a for a, _ in g], [b for _, b in g], where="post", color=SERIES[0], label="tasks_queued (вимірювач сервісу)")
        s = [(x["t"] - t0, x["queued"]) for x in p["samples"] if "queued" in x]
        if s:
            ax.plot([a for a, _ in s], [b for _, b in s], color=SERIES[1], lw=1.0, alpha=0.8, label="queued у базі (щосекунди)")
        ax.axvline(p["t_gen_end"] - t0, color=INK2, ls="--", lw=1.0)
        ax.set_xlabel("час від початку генерації, с")
        ax.set_ylabel("задач у черзі")
        ax.set_title(title)
        ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "little_queue.png"))
    plt.close(fig)


def plot_step9(f_runs):
    plt = setup_matplotlib()
    fig, axes = plt.subplots(len(f_runs), 1, figsize=(7.2, 3.0 * len(f_runs)), sharex=False, squeeze=False)
    titles = {"step9_fail015": "A: FAIL_RATE = 0,15, L = 5 с, heartbeat",
              "step9_f5_nohb": "B: Ф5, L = t_cpu/2, без heartbeat",
              "step9_f5_hb": "C: Ф5, L = t_cpu/2, з heartbeat"}
    for ax, (key, r) in zip(axes[:, 0], f_runs):
        t0 = r["t0"]
        g = [(x["t"] - t0, x["tasks_queued"]) for x in r["gauge"] if x["tasks_queued"] is not None]
        if g:
            ax.step([a for a, _ in g], [b for _, b in g], where="post", color=SERIES[0], label="tasks_queued")
        ax.set_ylabel("задач у черзі")
        ax.set_xlabel("час від початку генерації, с")
        ax2 = ax.twinx()
        ax2.spines["right"].set_visible(True)
        ax2.grid(False)
        ax2.plot([s - t0 for s in r["done_secs"]], r["done_per_s"], color=SERIES[1], lw=1.0, alpha=0.85,
                 label="завершено за секунду")
        ax2.set_ylabel("завершено за секунду")
        ax.axvline(r["t_gen_end"] - t0, color=INK2, ls=":", lw=1.0)
        ax.text(r["t_gen_end"] - t0, ax.get_ylim()[1] * 0.5, " кінець генерації, FAIL_RATE = 0", ha="left", va="center", fontsize=8)
        ax.set_title(titles[key])
        h1, l1 = ax.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "step9_timeline.png"))
    plt.close(fig)


if __name__ == "__main__":
    main()
