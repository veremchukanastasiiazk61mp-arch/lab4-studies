#!/usr/bin/env python3
"""Збирає звіт ЛР2 у Markdown (REPORT_LAB2.md) з результатів серії.

Усі числа беруться з results/<тег>analysis.json, manifest.json, noop.json, stop_check.json.
Текстові висновки формуються за цими числами. Скріншоти вставляються лише ті, що існують
у report/lab2/ (їхній перелік виводиться наприкінці), тому в тексті немає службових заготовок.

Запуск (після analysis/analyze.py):  python analysis/make_report.py [--tag cpu025_]
Потрібно: pip install matplotlib (для analyze.py); цей скрипт використовує лише стандартну бібліотеку.
"""
import argparse
import json
import os
import re
import shutil
import sys

sys.path.insert(0, os.path.dirname(__file__))
from analyze import fmt  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPO_URL = "https://github.com/veremchukanastasiiazk61mp-arch/lab1-studies"

SCREENSHOTS = [
    ("prometheus_targets.png", "Сторінка Targets у Prometheus: усі екземпляри web у стані UP", "targets"),
    ("docker_stop.png", "Вивід перевірки коректної зупинки (scripts/stop_check.py)", "stop"),
    ("docker_stats_xmax.png", "Вивід docker stats під час навантаження біля межі екземпляра (окремий прогін на одному екземплярі)", "stats"),
    ("prometheus_graph.png", "Графік інтенсивності запитів sum(rate(http_requests_total[30s])) у Prometheus під час прогону (сходинки)", "graph"),
]


def load(path, default=None):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def read_text(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def snippet(path, start, end):
    """Фрагмент файла від рядка, що починається з start, до рядка з end (включно)."""
    lines = read_text(os.path.join(ROOT, path)).splitlines()
    i = next(k for k, ln in enumerate(lines) if ln.startswith(start))
    j = next(k for k in range(i, len(lines)) if end in lines[k])
    return "\n".join(lines[i:j + 1])


def table(header, rows, text_first=False):
    aligns = ["---" if (i == 0 and text_first) else "---:" for i in range(len(header))]
    out = ["| " + " | ".join(header) + " |", "|" + "|".join(aligns) + "|"]
    return "\n".join(out + ["| " + " | ".join(str(c) for c in r) + " |" for r in rows])


def srv_text(q):
    if q is None:
        return "—"
    if q["beyond"]:
        return f"> {fmt(q['lower_ms'], 0)}"
    return fmt(q["estimate_ms"], 1)


def short(x):
    """Число з комою без зайвих нулів: 16,000 → 16; 0,125 → 0,125."""
    return f"{x:.3f}".rstrip("0").rstrip(".").replace(".", ",")


def bucket_text(q):
    if q is None:
        return "—"
    if q["beyond"]:
        return f"> {short(q['lower_ms'])} мс"
    return f"({short(q['lower_ms'])}; {short(q['upper_ms'])}] мс, похибка до {short(q['max_error_ms'])} мс"


def bottleneck(an, pool, limit_cpu):
    """Текст про спільний ресурс за виміряними числами; формулювання обережні."""
    s11 = an["step11"]
    n = s11["N"]
    p = next(x for x in an["points"] if x["N"] == n)
    m = p["metrics"]
    st = s11["docker_stats"] or {}
    groups = st.get("groups", {})
    ncpu = (an["environment"].get("docker") or {}).get("ncpu")
    lines = []
    web_share = m["cpu_share_of_limit"]
    infl = m["inflight_per_instance"]
    if web_share is not None:
        lines.append(f"Кожен екземпляр web на цій сходинці використовував {fmt(m['cpu_cores_per_instance'], 3)} ядра, "
                     f"тобто {fmt(web_share * 100, 0)} % ліміту {fmt(limit_cpu, 2)} CPU; у середньому в обробці було "
                     f"{fmt(infl, 1)} запитів на екземпляр при розмірі пулу {pool}.")
    cand = []
    if groups:
        parts = []
        for g in ("db", "lb"):
            if g in groups:
                parts.append(f"{g}: {fmt(groups[g]['cpu_cores'], 2)} ядра")
        if "web" in groups:
            parts.append(f"web: {fmt(groups['web']['cpu_cores'], 2)} ядра на екземпляр, {fmt(groups['web']['cpu_cores'] * n, 2)} разом")
        if st.get("generator_cpu_cores") is not None:
            parts.append(f"генератор: {fmt(st['generator_cpu_cores'], 2)} ядра")
        lines.append("Завантаження за docker stats: " + "; ".join(parts) + (f"; усього Docker бачить {ncpu} CPU." if ncpu else "."))
        total = sum(v["cpu_cores"] * (n if g == "web" else 1) for g, v in groups.items())
        total += st.get("generator_cpu_cores") or 0
        if ncpu:
            lines.append(f"Сумарно контейнери і генератор використовували близько {fmt(total, 1)} ядра з {ncpu} доступних.")
            if total >= 0.8 * ncpu or (st.get("host_cpu_percent") or 0) >= 85:
                cand.append("процесор хоста")
        if "db" in groups and groups["db"]["cpu_cores"] >= 0.8:
            cand.append("база даних PostgreSQL (CPU)")
        if "lb" in groups and groups["lb"]["cpu_cores"] >= 0.8:
            cand.append("балансувальник nginx")
    if st.get("host_cpu_percent") is not None:
        lines.append(f"Процесор хоста за psutil: {fmt(st['host_cpu_percent'], 0)} %, памʼять: {fmt(st['host_mem_percent'], 0)} %.")
    host = [h for h in (an.get("host") or {}).values() if h]
    if host:
        mem_lo = min(h["mem_mean"] for h in host)
        cpu_hi = max(h["cpu_max"] for h in host)
        lines.append(f"За весь час усіх прогонів серії памʼять хоста за psutil у середньому була {fmt(mem_lo, 0)}–"
                     f"{fmt(max(h['mem_mean'] for h in host), 0)} %, а найбільше завантаження процесора хоста в одній вибірці "
                     f"сягало {fmt(cpu_hi, 0)} % (вибірки раз на 5 с; докладніше нижче в таблиці 5).")
        if mem_lo >= 85 or cpu_hi >= 90:
            cand.append("ресурси самого комп'ютера, на якому працюють усі контейнери і генератор (памʼять і процесор хоста)")
    m2 = s11["m2_at_x"]
    total_conn = m2["active"] + m2["idle"]
    lines.append(f"За М2 на цій сходинці відкрито {fmt(total_conn, 1)} зʼєднань з бази (active {fmt(m2['active'], 1)}, "
                 f"idle {fmt(m2['idle'], 1)}) при межі N · пул = {n * pool}; база виконувала {fmt(m2['tps'], 0)} транзакцій за секунду.")
    if total_conn >= 0.9 * n * pool and m2["active"] >= 0.5 * total_conn:
        cand.append("пул зʼєднань і база (зʼєднання зайняті)")
    if cand:
        summary = ("прямих ознак насичення окремого контейнера немає (див. вище), а непрямі ознаки дозволяють припустити, що "
                   "зростання X(N) обмежує: " + "; ".join(cand) +
                   ". Це гіпотеза, а не доведений висновок: прогону з іншим розміром пулу або з базою на окремому хості не виконувалось")
    else:
        summary = ("жоден зі спільних ресурсів на сходинці X(N) не наближається до свого обмеження за виміряними показниками, "
                   "тому спільний ресурс за цими даними не визначено; потрібні додаткові вимірювання (інший розмір пулу, "
                   "окремий хост для бази)")
    lines.append(summary[0].upper() + summary[1:] + ".")
    return " ".join(lines), summary


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tag", default="")
    ap.add_argument("--results", default=os.path.join(ROOT, "results"))
    ap.add_argument("--out", default=os.path.join(ROOT, "REPORT_LAB2.md"))
    a = ap.parse_args()
    res, tag = a.results, a.tag

    an = load(os.path.join(res, f"{tag}analysis.json"))
    if an is None:
        sys.exit("Спершу виконайте: python analysis/analyze.py")
    manifest = load(os.path.join(res, f"{tag}manifest.json"), {})
    stop = load(os.path.join(res, "stop_check.json"))
    noop = load(os.path.join(res, f"{tag}noop.json"))
    env = an["environment"]
    docker = env.get("docker") or {}
    limits = env.get("web_limits", {})
    limit_cpu = float(limits.get("WEB_CPUS", "0.5"))
    pool = int(limits.get("DB_POOL_SIZE", "15"))
    g = env["G_rps"]
    points = an["points"]
    u = an["usl"]
    img_dir = os.path.join(ROOT, "report", "lab2")
    os.makedirs(img_dir, exist_ok=True)

    def figure(name, caption):
        src = os.path.join(res, "figures", f"{tag}{name}")
        if not os.path.exists(src):
            return ""
        shutil.copyfile(src, os.path.join(img_dir, f"{tag}{name}"))
        return f"![{caption}](report/lab2/{tag}{name})\n\n*{caption}*\n"

    shots, missing = {}, []
    for fn, cap, key in SCREENSHOTS:
        if os.path.exists(os.path.join(img_dir, fn)):
            shots[key] = f"![{cap}](report/lab2/{fn})\n\n*{cap}*\n"
        else:
            missing.append(fn)
    fig_no = [0]

    def fig_caption(text):
        fig_no[0] += 1
        return f"Рис. {fig_no[0]}. {text}"

    L = []
    w = L.append
    w("""МІНІСТЕРСТВО ОСВІТИ І НАУКИ УКРАЇНИ
НАЦІОНАЛЬНИЙ ТЕХНІЧНИЙ УНІВЕРСИТЕТ УКРАЇНИ «КИЇВСЬКИЙ ПОЛІТЕХНІЧНИЙ ІНСТИТУТ ІМЕНІ ІГОРЯ СІКОРСЬКОГО»
Факультет біомедичної інженерії, кафедра біомедичної кібернетики

# Лабораторна робота №2

**з дисципліни «Високопродуктивні розподілені обчислювальні системи»**
**на тему: «Метрики сервісу і межа масштабованості»**

Виконала: студентка гр. ЗК-61мп Веремчук Анастасія
Прийняв: ст. викл. каф. БМК Матвійчук О.В.

Репозиторій: """ + REPO_URL + """

Київ – 2026

---
""")
    w("## Мета\n\nДоповнити сервіс із лабораторної роботи 1 метриками у форматі Prometheus, розгорнути Prometheus у "
      "Docker Compose, виміряти пропускну здатність і затримку сервісу ступінчастим навантаженням на 1, 2, 3 і 4 "
      "екземплярах та за результатами визначити параметри універсального закону масштабованості.\n")
    w("## Варіант 5\n")
    w(table(["Параметр", "Значення"], [
        ["Розмір пулу зʼєднань (DB_POOL_SIZE)", pool],
        ["Набір бакетів гістограми", "Г3: 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024 мс"],
        ["Додаткова метрика", "М2: postgres-exporter; зʼєднання за станом і транзакції за секунду"],
        ["Ліміт контейнера web", f"{limits.get('WEB_CPUS', '0.5')} CPU, {limits.get('WEB_MEMORY', '512M')}"],
    ], text_first=True) + "\n")

    w("## 1. Стенд\n")
    mem = docker.get("mem_total_bytes")
    w(f"Вимірювання виконано на одному комп'ютері: генератор навантаження, nginx, база даних, Prometheus і всі "
      f"екземпляри web працювали разом. Docker виділяє {docker.get('ncpu', '—')} CPU і "
      f"{fmt(mem / 2**30, 2) if mem else '—'} ГіБ памʼяті (Docker {docker.get('server_version', '—')}, "
      f"Compose {docker.get('compose_version', '—')}). Ліміт контейнера web: {limits.get('WEB_CPUS', '0.5')} CPU і "
      f"{limits.get('WEB_MEMORY', '512M')}; у ході вимірювань він не змінювався"
      + (f" (серія з тегом `{tag}`)" if tag else "") + ".\n")
    w("Склад Compose: `db` (PostgreSQL 16), `web` (кількість задає `WEB_REPLICAS`), `lb` (nginx), `prometheus` (збір раз на 5 с) "
      "і `postgres-exporter` (М2). Після кожної зміни кількості екземплярів балансувальник перезапускається, щоб отримати "
      "нові адреси.\n")

    w("## 2. Реалізація\n")
    w("### 2.1 Метрики Prometheus (крок 1)\n")
    w("Метрики реєструються один раз під час запуску сервісу (`service/app/metrics.py`) і оновлюються у проміжному "
      "ASGI-обробнику. Мітка `path` містить шаблон маршруту (`/studies/{study_id}`), а не ідентифікатор, щоб кількість "
      "рядів не росла з кількістю записів. Ендпоінт `/metrics` не враховується у власних метриках. Сервіс запускається одним "
      "процесом (`HTTP_WORKERS` має дорівнювати 1, інакше конфігурація відхиляється).\n")
    try:
        w("```python\n" + snippet("service/app/metrics.py", "REQUESTS = Counter(", "IN_FLIGHT = Gauge(") + "\n```\n")
    except (StopIteration, FileNotFoundError):
        pass
    w("Межі бакетів задаються змінною `HISTOGRAM_BUCKETS_MS=1,2,4,8,16,32,64,128,256,512,1024` (набір Г3) у мілісекундах і "
      "перераховуються в секунди (межа 1 мс має мітку `le=\"0.001\"`).\n")
    w("### 2.2 Додаткові ендпоінти і змінні (крок 2)\n")
    w("`GET /livez` повертає 200 без звернення до бази. Кожна відповідь має заголовок `X-Pod` з імʼям екземпляра: "
      "змінна `POD_NAME`, а якщо її не задано, імʼя хоста контейнера. Розмір пулу задає `DB_POOL_SIZE`; пул asyncpg "
      f"не має переповнення, тому екземпляр відкриває не більше {pool} зʼєднань з базою.\n")
    w("### 2.3 Коректна зупинка (крок 3)\n")
    w("`CMD` у Dockerfile записано в exec-формі, тому процес сервісу є PID 1 і отримує SIGTERM від `docker stop`. Стандартний "
      "uvicorn після зупинки повторно надсилає собі отриманий сигнал і завершується з кодом 143, тому в `app/__main__.py` "
      "використано власний клас сервера: сигнал лише запускає штатну зупинку (нові зʼєднання не приймаються, запити в "
      "обробці дочікуються, пул зʼєднань закривається), після чого процес завершується з кодом 0. У `docker-compose.yml` "
      "задано `stop_grace_period: 30s`.\n")
    try:
        w("```python\n" + snippet("service/app/__main__.py", "class GracefulServer", "навмисно без") + "\n```\n")
    except (StopIteration, FileNotFoundError):
        pass
    if stop:
        wnd = stop["window"]
        w(f"Перевірка (`python scripts/stop_check.py`): на двох екземплярах web при постійному навантаженні {stop.get('load_rps', 60)} запитів за секунду "
          f"виконано `docker stop` для контейнера `{stop['container']}`. Команда тривала {fmt(stop['docker_stop_seconds'], 2)} с, "
          f"код завершення контейнера {stop['exit_code']} (`docker inspect --format '{{{{.State.ExitCode}}}}'`). У вікні ±10 с навколо "
          f"зупинки генератор зафіксував {wnd['ok_2xx']} відповідей 2xx, {wnd['non_2xx']} відповідей не 2xx і {wnd['conn_errors']} "
          f"помилок зʼєднання; за весь прогін: не 2xx {stop['whole_run']['non_2xx']}, помилок зʼєднання "
          f"{stop['whole_run']['conn_errors']}. Результат: {'умову виконано' if stop['passed'] else 'умову НЕ виконано'}.\n")
        if "stop" in shots:
            w(shots["stop"])
    w("### 2.4 Prometheus (крок 4)\n")
    w("Prometheus збирає метрики раз на 5 с; екземпляри web знаходить через `dns_sd_configs` (`type: A`, порт сервісу, "
      "`refresh_interval: 5s`), дані зберігає в іменованому томі `promdata`, порт 9090 опубліковано на хост. Для М2 "
      "додано `postgres-exporter` із власним запитом `pg_conn` до `pg_stat_activity` (метрика `pg_conn_count{state}`); "
      "кількість транзакцій береться зі стандартної метрики `pg_stat_database_xact_commit` і `..._xact_rollback`.\n")
    if "targets" in shots:
        w(shots["targets"])
    w("### 2.5 Генератор (кроки 5–6)\n")
    w("`loadgen/stepload.py` — генератор відкритого контуру: моменти відправки обчислюються наперед, затримка рахується від "
      "запланованого моменту, тайм-аут запиту 5 с (тайм-аут рахується як відповідь не 2xx). Профіль: прогрів 10 с на рівні першої сходинки, далі сходинки по 20 с із приростом, що дорівнює першій "
      f"сходинці: {manifest.get('step_rate', 200)} запитів за секунду для прогонів проти web (для `/noop` — 200). Кожну секунду записуються задана і фактична інтенсивність "
      "надсилання, досягнута інтенсивність, кількість відповідей 2xx і не 2xx, помилки зʼєднання, p50, p95 і p99 клієнта "
      "(файли `results/*_seconds.csv`). Прогін зупиняється, коли на двох сходинках поспіль досягнуто менше 0,9 заданої або "
      "сходинка досягла межі генератора G. Досягнута інтенсивність сходинки — середнє кількості 2xx за секунду за останні "
      "15 с (перші 5 с перехідні); X(N) — найбільша серед сходинок, розкид — вибіркове стандартне відхилення за ті самі 15 с.\n")
    if noop:
        w(f"Прогін проти `/noop` через той самий опублікований порт (nginx відповідає 204 без звернення до web) дав межу "
          f"генератора G = {fmt(g, 1)} запитів за секунду (розкид {fmt(noop['X_std_rps'], 1)}; процесів генератора "
          f"{noop['config']['procs']}; зупинка: {noop['stop_reason']}). Усі прогони серії виконано з тим самим числом процесів.\n")
    else:
        w(f"Межа генератора за прогоном проти `/noop`: G = {fmt(g, 1)} запитів за секунду.\n")

    w("## 3. Результати вимірювань (крок 7)\n")
    rows = []
    for p in points:
        m = p["metrics"]
        rows.append([p["N"], fmt(p["X"], 1), fmt(p["std"], 1), f"{p['step']} ({p['offered']} rps)",
                     fmt(m["p99_client_ms"], 1), srv_text(m["server_p99"]), fmt((m["non_2xx_share"] or 0) * 100, 2),
                     fmt(m["cpu_cores_per_instance"], 3), fmt(m["inflight_per_instance"], 1),
                     "так" if p["generator_limited"] else "ні"])
    w("Таблиця 1. X(N), його розкид і показники сходинки, на якій досягнуто X(N)\n")
    w(table(["N", "X(N), rps", "розкид, rps", "сходинка", "p99 клієнта, мс", "p99 сервера, мс", "не 2xx, %",
             "CPU на екз., ядер", "запитів в обробці на екз.", "обмежено генератором"], rows) + "\n")
    if any(len(p.get("X_runs", [])) > 1 for p in points):
        w("Таблиця 1 наведена для основної серії. Серію виконано двічі (перша серія і основна), а для N = 1 додатково виконано ще два повтори. Окремі прогони тих самих N дали помітно різні значення (особливо N = 1), "
          "тому нижче наведено всі прогони; для підбору закону використано медіану за всіма прогонами.\n")
        rows_r = [[p["N"], "; ".join(f"{fmt(r['X'], 1)} ({r['source']})" for r in p["X_runs"]), len(p["X_runs"]),
                   fmt(p["X_median"], 1), f"{fmt(p['X_min'], 1)}–{fmt(p['X_max'], 1)}"] for p in points]
        w("Таблиця 1а. Усі прогони і медіана X(N)\n")
        w(table(["N", "прогони X(N), rps", "кількість", "медіана, rps", "мінімум–максимум"], rows_r) + "\n")
        w("Розкид між прогонами (наприклад, для N = 1 від "
          f"{fmt(points[0]['X_min'], 0)} до {fmt(points[0]['X_max'], 0)} запитів за секунду) більший за розкид усередині одного "
          "прогону (таблиця 1). Отже, межа екземпляра у цьому стенді відтворюється погано: біля насичення затримка різко зростає, "
          "і точка, де прогін «обвалюється», змінюється від запуску до запуску. Одиночний вимір не можна вважати точною "
          "характеристикою, тому для N = 1 далі використано медіану.\n")
    lim = [p for p in points if p["generator_limited"]]
    if lim:
        w("Обмежені генератором точки (" + ", ".join(f"N = {p['N']}: {'; '.join(p['limited_reasons'])}" for p in lim) +
          ") не входять у підбір параметрів закону.\n")
    else:
        w(f"Жодна точка не обмежена генератором: X(N) < 0,9 · G = {fmt(0.9 * g, 1)}, а на останній сходинці кожного прогону "
          "досягнуто менше 0,9 заданої.\n")
    w("p99 сервера оцінено за приростами лічильників бакетів гістограми за останні 15 с сходинки, просумованими по всіх "
      "екземплярах; приріст на межах вікна обчислено лінійною інтерполяцією між двома найближчими вибірками Prometheus "
      "(інтервал збору 5 с).\n")
    ns = sorted(p["N"] for p in points)
    w(figure(f"step8_n{ns[0]}.png", fig_caption(f"Прогін на {ns[0]} екземплярі: інтенсивність, p99, запити, CPU і запити в обробці")))
    if ns[-1] != ns[0]:
        w(figure(f"step8_n{ns[-1]}.png", fig_caption(f"Прогін на {ns[-1]} екземплярах: ті самі величини для кожного екземпляра")))
    w("Екземпляри на діаграмах пронумеровано за відсортованими адресами; кожен має свій колір, ліміт CPU і розмір пулу "
      "показано горизонтальними лініями.\n")
    if "graph" in shots:
        w(shots["graph"])

    w("## 4. p99 клієнта і сервера на одному екземплярі (крок 9)\n")
    rows = [[s["step"], s["offered"], fmt(s["achieved"], 1), fmt(s["p99_client_ms"], 1), srv_text(s["server_p99"]),
             bucket_text(s["server_p99"])] for s in an["step9"]]
    w("Таблиця 2. Порівняння p99 клієнта і p99 сервера на кожній сходинці прогону на одному екземплярі\n")
    w(table(["сходинка", "задано, rps", "досягнуто, rps", "p99 клієнта, мс", "p99 сервера, мс", "бакет і найбільша похибка"], rows) + "\n")
    beyond = [s["step"] for s in an["step9"] if s["server_p99"] and s["server_p99"]["beyond"]]
    inside = [s for s in an["step9"] if s["server_p99"] and not s["server_p99"]["beyond"]]
    txt = ("p99 сервера не містить часу в мережі та в черзі балансувальника, а p99 клієнта містить, тому на сходинках після "
           "насичення p99 клієнта значно більший. ")
    if inside:
        ratios = [s["p99_client_ms"] / s["server_p99"]["estimate_ms"] for s in inside if s["server_p99"]["estimate_ms"]]
        if ratios:
            if min(ratios) >= 1:
                txt += f"Поки екземпляр не насичений, p99 клієнта більший за p99 сервера у {fmt(min(ratios), 1)}–{fmt(max(ratios), 1)} раза. "
            else:
                txt += (f"Поки екземпляр не насичений, відношення p99 клієнта до p99 сервера лежить у межах {fmt(min(ratios), 2)}–"
                        f"{fmt(max(ratios), 2)}; оцінка сервера грубіша за виміряне значення клієнта через ширину бакетів. ")
    if beyond:
        txt += (f"На сходинках {', '.join(map(str, beyond))} ранг p99 потрапив за останню скінченну межу гістограми (1024 мс), "
                "тому оцінка дорівнює цій межі, а похибка зверху не обмежена; у таблиці це позначено «> 1024». ")
    w(txt + "Похибка оцінки всередині бакета не перевищує його ширини, а бакети Г3 подвоюються, тому на високих значеннях "
      "оцінка груба.\n")

    w("## 5. Універсальний закон масштабованості (крок 10)\n")
    if "sigma" in u:
        rows = [[r["N"], fmt(r["X"], 1), fmt(r["C"], 3), fmt(r["y"], 4), fmt(r["X_pred"], 1), fmt(r["rel_err"] * 100, 1)]
                for r in u["points"]]
        w("Таблиця 3. Підбір σ і κ методом найменших квадратів без вільного члена\n")
        w(table(["N", "X(N), rps", "C(N)", "y = N/C − 1", "X за законом, rps", "відхилення, %"], rows) + "\n")
        par = (f"λ = X(1) = {fmt(u['lambda'], 1)} rps. Оцінки: σ = {fmt(u['sigma'], 4)}, κ = {fmt(u['kappa'], 5)} "
               f"(метод: {u['method']}; у підборі N = {', '.join(map(str, u['points_used']))}).")
        if u["N_star"]:
            par += f" Максимум пропускної здатності при N* = √((1 − σ)/κ) = {fmt(u['N_star'], 2)}, X(N*) = {fmt(u['X_at_N_star'], 1)} rps."
            if u["N_star"] > max(ns):
                par += f" Це за межами виміряного діапазону N ≤ {max(ns)}, тому N* є екстраполяцією."
        elif u["X_limit"]:
            par += f" Оскільки κ = 0, максимуму немає, X(N) наближається до λ/σ = {fmt(u['X_limit'], 1)} rps."
        else:
            par += " Оскільки σ = κ = 0, масштабування лінійне."
        w(par + "\n")
        sf = an.get("series_fits") or []
        if sf:
            rows_s = []
            for r in sf:
                if "error" in r:
                    rows_s.append([r["label"], "—", "—", "—", "—", r["error"]])
                else:
                    xs_s = ", ".join(f"{k}: {fmt(v, 0)}" for k, v in r["X"].items())
                    rows_s.append([r["label"], xs_s, fmt(r["sigma"], 4), fmt(r["kappa"], 5),
                                   fmt(r["N_star"], 2) if r["N_star"] else "—", r["method"]])
            w("Таблиця 3а. Чутливість підбору до вибору серії (кожна серія окремо)\n")
            w(table(["серія", "X(N), rps", "σ", "κ", "N*", "метод"], rows_s, text_first=True) + "\n")
            w("Значення σ, κ і N* помітно залежать від того, яку серію взято, а відхилення моделі від медіан сягають понад 20 %. "
              "Тому параметри закону слід вважати грубою оцінкою, а не точною характеристикою сервісу: точок мало (три точки з "
              "N > 1), а розкид між прогонами порівнянний з відмінностями між сусідніми N.\n")
        w(figure("usl.png", fig_caption("Виміряні X(N) з розкидом, крива закону і пряма λN")))
    else:
        w(f"Параметри закону не підібрано: {u['error']}.\n")

    w("## 6. Спільний ресурс, що обмежує зростання X(N) (крок 11)\n")
    s11 = an["step11"]
    st = s11["docker_stats"]
    if st:
        rows = [[gname, fmt(v["cpu_cores"], 2), fmt(v["mem_mib"], 0)] for gname, v in sorted(st["groups"].items())]
        w(f"Таблиця 4. Використання ресурсів контейнерами на сходинці {s11['step']}, де досягнуто X({s11['N']}) "
          f"(`docker stats`, середнє за вибірками у вікні, на один контейнер)\n")
        w(table(["контейнер", "CPU, ядер", "памʼять, МіБ"], rows, text_first=True) + "\n")
    bn_text, bn_summary = bottleneck(an, pool, limit_cpu)
    w(bn_text + "\n")
    hostd = an.get("host") or {}
    if any(hostd.values()):
        rows_h = [[n, h["samples"], fmt(h["cpu_mean"], 0), fmt(h["cpu_max"], 0), fmt(h["mem_mean"], 0), fmt(h["mem_max"], 0)]
                  for n, h in sorted(hostd.items(), key=lambda kv: int(kv[0])) if h]
        w("Таблиця 5. Завантаження хоста (psutil) за весь час прогону основної серії, %\n")
        w(table(["N", "вибірок", "CPU середнє", "CPU максимум", "памʼять середня", "памʼять максимум"], rows_h) + "\n")
        w("Вимірювання виконано на ноутбуці, де памʼять була зайнята майже повністю, а процесор періодично навантажувався "
          "майже на 100 %. Це могло обмежити як зростання X(N) при N ≥ 2, так і відтворюваність прогонів. Виміряне не "
          "дозволяє довести це напевно: щоб підтвердити, потрібен прогін на комп'ютері з більшою памʼяттю або з базою на окремому хості.\n")
    if "stats" in shots:
        w(shots["stats"])
    w(figure("m2.png", fig_caption(f"М2: зʼєднання з базою за станом і транзакції за секунду залежно від заданої інтенсивності (N = {ns[0]} і N = {ns[-1]})")))

    w("## 7. Відкинуті рішення\n")
    w("1. **Багатопроцесний режим prometheus_client.** Відкинуто: вимагає каталогу для файлів метрик і ускладнює розподіл "
      "ресурсів; завдання прямо вимагає один процес у контейнері.\n"
      "2. **Стандартна поведінка uvicorn при SIGTERM.** Відкинуто: процес завершується з кодом 143, а не 0, хоч запити "
      "доопрацьовуються.\n"
      "3. **Окремий скрейп кожного екземпляра за статичними адресами.** Відкинуто: адреси реплік змінюються, тому "
      "використано `dns_sd_configs` з оновленням раз на 5 с.\n"
      "4. **Інтерполяція `query_range` у Prometheus для діаграм.** Відкинуто: сирі вибірки з їхніми справжніми часовими "
      "мітками вивантажуються у JSON (`scripts/prom_export.py`), щоб розрахунки і діаграми відтворювались і після `docker compose down -v`; "
      "зсув годинника між хостом і віртуальною машиною Docker вимірюється і враховується.\n"
      "5. **Багатопотоковий генератор.** Відкинуто на користь asyncio: під час насичення в обробці перебувають тисячі "
      "запитів (інтенсивність · тайм-аут 5 с), а потоки на кожен із них були б надто дорогими.\n")

    w("## 8. Запис про використання генеративних моделей\n")
    w("Використано модель Claude (Anthropic). Застосовано для каркасу змін у сервісі (метрики, `/livez`, `X-Pod`, пул, "
      "зупинка за SIGTERM, перевірка символу `\\u0000`), конфігурації Compose і Prometheus, генератора ступінчастого навантаження, "
      "скриптів серії та аналізу, розрахункового модуля (підбір σ і κ, оцінка квантиля за гістограмою) і структури цього звіту.\n")
    w("Що перевірено під час створення (без Docker): юніт-тести валідації, конфігурації і проміжного обробника метрик; "
      "розрахункові функції на відомих відповідях (відновлення заданих σ і κ, оцінка квантиля за ручним прикладом); "
      "генератор проти заглушки сервера з відомою пропускною здатністю; поведінка SIGTERM (код завершення 0 і дочікування "
      "запиту в обробці) на тестовому ASGI-застосунку; конвеєр аналізу на синтетичних даних.\n")
    w("Що модель не перевіряла: збірку образу, роботу Compose, Prometheus і postgres-exporter, жодного реального вимірювання. "
      "Усі числа в цьому звіті отримано з прогонів на моєму комп'ютері, скрипти `scripts/stop_check.py`, `scripts/run_series.py` "
      "і `analysis/analyze.py` запускала я.\n")

    w("## Висновки\n")
    concl = [f"Сервіс доповнено метриками Prometheus (лічильник запитів, гістограма тривалості з бакетами Г3, вимірювач запитів "
             f"в обробці), ендпоінтом `/livez`, заголовком `X-Pod`, розміром пулу `DB_POOL_SIZE = {pool}` і коректною зупинкою за SIGTERM."]
    if stop:
        concl[-1] += (f" Зупинку одного з двох екземплярів під навантаженням виконано з кодом завершення {stop['exit_code']}; "
                      f"у вікні ±10 с відповідей не 2xx {stop['window']['non_2xx']}, помилок зʼєднання {stop['window']['conn_errors']}.")
    if any(len(p.get("X_runs", [])) > 1 for p in points):
        xs = ", ".join(f"X({p['N']}) = {fmt(p['X_median'], 0)} ({fmt(p['X_min'], 0)}–{fmt(p['X_max'], 0)}, прогонів {len(p['X_runs'])})"
                       for p in points)
        concl.append(f"Пропускна здатність (медіана за прогонами, у дужках діапазон): {xs} запитів за секунду при ліміті "
                     f"{fmt(limit_cpu, 2)} CPU на екземпляр; межа генератора G = {fmt(g, 0)}. Прогони тих самих N відрізняються "
                     "помітно, особливо для N = 1, тому точність оцінок обмежена.")
    else:
        xs = ", ".join(f"X({p['N']}) = {fmt(p['X'], 0)}" for p in points)
        concl.append(f"Пропускна здатність: {xs} запитів за секунду при ліміті {fmt(limit_cpu, 2)} CPU на екземпляр; межа генератора G = {fmt(g, 0)}.")
    if "sigma" in u:
        concl.append(f"За універсальним законом (за медіанами) σ = {fmt(u['sigma'], 4)}, κ = {fmt(u['kappa'], 5)}"
                     + (f", максимум при N* = {fmt(u['N_star'], 1)} (за межею виміряних N ≤ {max(ns)})" if u["N_star"] and u["N_star"] > max(ns)
                        else (f", максимум при N* = {fmt(u['N_star'], 1)}" if u["N_star"] else ""))
                     + "; ці оцінки грубі (див. таблицю 3а).")
    else:
        concl.append("Параметри закону за цією серією не підібрано (" + u["error"] + ").")
    concl.append("Спільний ресурс: " + bn_summary + ".")
    for i, c in enumerate(concl, 1):
        w(f"{i}. {c}")
    w("")

    text = "\n".join(L)
    text = re.sub(r"\n{3,}", "\n\n", text)
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"Звіт: {a.out}")
    if missing:
        print("Щоб додати скріншоти до звіту, збережіть їх у report/lab2/ під цими іменами і запустіть скрипт ще раз:")
        for fn, cap, _ in SCREENSHOTS:
            if fn in missing:
                print(f"  {fn}: {cap}")


if __name__ == "__main__":
    main()
