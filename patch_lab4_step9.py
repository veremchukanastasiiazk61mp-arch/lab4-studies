# Правка scripts/lab4.py для кроку 9: нова версія fail_run і cmd_step9 (див. пояснення в чаті).
p = "scripts/lab4.py"
s = open(p, encoding="utf-8").read()
a = s.index("def fail_run(")
b = s.index("def cmd_d2(")
NEW = r'''def fail_run(label, xr, lease, heartbeat, duration, fail_rate=0.15):
    """Прогін кроку 9: інтенсивність 0,7·X(R), FAIL_RATE = fail_rate, воркерів R, L і heartbeat задано.

    Порядок: (1) базовий стан без падінь і порожня черга, (2) воркерам задається FAIL_RATE, L, heartbeat (поди
    стартують на порожній черзі, тому стають готовими), (3) генерація duration секунд, (4) ЗНІМОК стану в кінці
    генерації: розподіл attempts за задачами, що вже завершені (done або failed), перезапуски контейнерів, рядки
    журналу «0 rows updated», (5) FAIL_RATE повертається в 0 і черга дочищається (інакше Kubernetes, збільшуючи паузу
    між перезапусками контейнерів, що падають, до 5 хв, лишив би чергу недоопрацьованою), (6) підсумкові
    перевірки (дублі результатів) після спорожнення."""
    default_env(R)                                     # без падінь, типові L і heartbeat
    reset_tasks()
    pods = ENV.set_env(R, FAIL_RATE=fail_rate, LEASE_SECONDS=lease, HEARTBEAT="true" if heartbeat else "false")
    time.sleep(2)
    sampler = Sampler(with_gauge=True)
    sampler.start()
    t0 = time.time()
    s = taskgen(os.path.join(RES, f"{label}_tasks.json"), rate=0.7 * xr, duration=duration)
    t_gen_end = time.time()

    # ---- знімок у кінці генерації (до зміни FAIL_RATE)
    pods_now = ENV.pods()
    restarts = {p: r for p, _, r in pods_now}
    zero_rows = sum(ENV.log_grep_count(p, "0 rows updated") for p, _, _ in pods_now)
    dist = ENV.sql("SELECT attempts, count(*) FROM tasks WHERE status IN ('done', 'failed') GROUP BY 1 ORDER BY 1")
    snap = ENV.sql("SELECT count(*) FILTER (WHERE status = 'queued'), count(*) FILTER (WHERE status = 'running'), "
                   "count(*) FILTER (WHERE status = 'done'), count(*) FILTER (WHERE status = 'failed'), "
                   "count(*) FILTER (WHERE status IN ('done', 'failed') AND attempts > 1), count(*) FROM tasks")
    q_, r_, d_, f_, gt1, total = (int(x) for x in snap.split("|"))
    terminal = d_ + f_
    attempts_dist = {int(l.split("|")[0]): int(l.split("|")[1]) for l in dist.splitlines()}

    # ---- дочищення черги без падінь
    ENV.set_env(R, FAIL_RATE="0", LEASE_SECONDS=LEASE_S, HEARTBEAT="true")
    drained = wait_drain(T(900))
    t_end = time.time()
    sampler.stop()
    n, dup, _ = dup_stats()
    secs, counts = per_second_done(t0, t_end)
    res = {"label": label, "fail_rate": fail_rate, "lease_s": lease, "heartbeat": heartbeat, "rate": 0.7 * xr,
           "duration": duration, "t0": t0, "t_gen_end": t_gen_end, "t_end": t_end, "drained": drained,
           "restarts": restarts, "restarts_total": sum(restarts.values()),
           "snapshot_at_generation_end": {"queued": q_, "running": r_, "done": d_, "failed": f_, "tasks": total,
                                          "terminal": terminal, "attempts_gt1_among_terminal": gt1},
           "attempts_distribution_terminal": attempts_dist,
           "tasks": n, "failed_final": int(ENV.sql("SELECT count(*) FROM tasks WHERE status = 'failed'")),
           "n_dup_results": dup, "zero_rows_log_lines": zero_rows,
           "done_in_window": d_, "X_run": d_ / duration,
           "done_secs": secs, "done_per_s": counts, "samples": sampler.rows, "gauge": sampler.gauge}
    share = gt1 / terminal if terminal else float("nan")
    share_failed = f_ / terminal if terminal else float("nan")
    print(f"   {label}: у кінці генерації завершено {terminal} із {total} задач, X = {d_ / duration:.2f} задач/с; "
          f"перезапусків контейнерів {res['restarts_total']}; attempts>1: {gt1}/{terminal} ({share:.3f}); "
          f"failed {f_}/{terminal} ({share_failed:.3f}); дублів результатів {dup}; рядків «0 rows updated» {zero_rows}")
    save(label, res)
    return res


def cmd_step9(a):
    ENV.sql_file(os.path.join(ROOT, "db", "lab4", "restore_results.sql"))
    st = load_state()
    xr = a.xr or st.get(f"X{R}")
    if not xr:
        sys.exit("потрібне X(R): виконайте step6 або вкажіть --xr")
    t_cpu = (a.t_cpu_ms or st.get("t_cpu_ms"))
    if not t_cpu:
        sys.exit("потрібне t_cpu: виконайте step2 або вкажіть --t-cpu-ms")
    dur = T(300)
    # A. FAIL_RATE = 0,15, L варіанта з heartbeat (без Ф5): порівняння X і розподілу attempts з X(R) без падінь
    fail_run("step9_fail015", xr, LEASE_S, True, dur)
    # B, C. Сценарій Ф5: L = t_cpu / 2, спершу без heartbeat (B), далі з heartbeat (C); FAIL_RATE = 0,15 в обох
    lease = t_cpu / 1000 / 2
    fail_run("step9_f5_nohb", xr, lease, False, dur)
    fail_run("step9_f5_hb", xr, lease, True, dur)
    default_env(R)   # повертаємо типові значення


'''
s = s[:a] + NEW + s[b:]
open(p, "w", encoding="utf-8").write(s)
print("готово")
