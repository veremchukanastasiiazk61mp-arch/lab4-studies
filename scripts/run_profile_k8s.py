#!/usr/bin/env python3
"""ЛР3, кроки 10–11: прогін із профілем «0,3·X(1) → стрибок → 0,3·X(1)» без автомасштабування і з HPA.

  python scripts/run_profile_k8s.py --mode fixed                 # крок 10: R = 5 реплік без HPA
  python scripts/run_profile_k8s.py --mode hpa                   # крок 11: HPA (ціль 70 %, вікно 30 с)
  python scripts/run_profile_k8s.py --mode hpa --init-delay 15   # Д6: початкова ініціалізація 15 с

Профіль: 30 с на рівні 0,3·X(1), 60 с на рівні стрибка, 90 с на рівні 0,3·X(1).
Рівень стрибка = min(2,5·X(1), 0,9·X(R)). X(1) і X(R) беруться з results/step_n1.json і step_nR.json.

Режим fixed: масштабує Deployment до R і чекає готовності. Режим hpa: прибирає HPA, масштабує до 1,
створює HPA (k8s/hpa.yaml) і чекає, поки `kubectl get hpa` показує одну репліку 60 секунд поспіль.
Перед кожним прогоном виставляє STARTUP_DELAY_SECONDS (Д6) і, якщо значення змінилось, перезапускає web.
"""
import argparse
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
import prom_export  # noqa: E402
import run_series_k8s as rs  # noqa: E402

ROOT, RES, R = rs.ROOT, rs.RES, rs.R


def levels(tag):
    x1 = json.load(open(os.path.join(RES, f"{tag}step_n1.json")))["X_rps"]
    xr = json.load(open(os.path.join(RES, f"{tag}step_n{R}.json")))["X_rps"]
    base = round(0.3 * x1)
    jump = round(min(2.5 * x1, 0.9 * xr))
    return x1, xr, base, jump


def set_init_delay(n):
    cm = json.loads(rs.kubectl("-n", "studies", "get", "configmap", "web-config", "-o", "json"))["data"]
    if cm["STARTUP_DELAY_SECONDS"] == str(n):
        return
    rs.kubectl("-n", "studies", "patch", "configmap", "web-config", "--type", "merge",
               "-p", json.dumps({"data": {"STARTUP_DELAY_SECONDS": str(n)}}))
    rs.kubectl("-n", "studies", "rollout", "restart", "deployment/web")
    rs.kubectl("-n", "studies", "rollout", "status", "deployment/web", "--timeout=240s")
    print(f"  STARTUP_DELAY_SECONDS = {n}, web перезапущено", flush=True)


def hpa_state():
    d = json.loads(rs.kubectl("-n", "studies", "get", "hpa", "web", "-o", "json"))["status"]
    return d.get("currentReplicas"), d.get("desiredReplicas")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["fixed", "hpa"], required=True)
    ap.add_argument("--init-delay", type=int, default=0, help="Д6: початкова ініціалізація пода, с (0 або 15)")
    ap.add_argument("--series-tag", default="", help="префікс файлів серії з кроку 7")
    ap.add_argument("--name", help="імʼя прогону (за замовчуванням profile_fixed / profile_hpa / profile_hpa_init15)")
    ap.add_argument("--base", default="http://localhost:8080")
    ap.add_argument("--prom", default="http://localhost:9090")
    ap.add_argument("--procs", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    a = ap.parse_args()
    name = a.name or ("profile_fixed" if a.mode == "fixed" else "profile_hpa" + (f"_init{a.init_delay}" if a.init_delay else ""))

    x1, xr, base_rate, jump = levels(a.series_tag)
    profile = f"30:{base_rate},60:{jump},90:{base_rate}"
    print(f"X(1) = {x1}, X({R}) = {xr}: базовий рівень {base_rate}, рівень стрибка {jump} rps; профіль {profile}", flush=True)

    os.makedirs(RES, exist_ok=True)
    rs.kubectl("-n", "studies", "delete", "hpa", "web", "--ignore-not-found")
    set_init_delay(a.init_delay)
    if a.mode == "fixed":
        rs.scale_to(R, a.base, a.prom)
    else:
        rs.scale_to(1, a.base, a.prom)
        subprocess.run([*rs.kubectl_cmd(), "apply", "-f", "-"], input=open(os.path.join(ROOT, "k8s", "hpa.yaml")).read(),
                       text=True, check=True, cwd=ROOT)
        print("  HPA створено; чекаю, поки він показує одну репліку 60 с поспіль...", flush=True)
        calm = 0
        for _ in range(600):
            cur, des = hpa_state()
            calm = calm + 1 if (cur == 1 and des == 1) else 0
            if calm >= 60:
                break
            time.sleep(1)
        else:
            sys.exit("HPA не заспокоївся на одній репліці")
    rs.ensure_ids(a.base)

    sampler = rs.HostSampler(os.path.join(RES, f"{name}_host.jsonl"))
    sampler.start()
    t_begin = time.time()
    try:
        t0, res = rs.stepload(argparse.Namespace(base=a.base, procs=a.procs), name, ["--profile", profile, "--transient", "10"])
    finally:
        sampler.stop_flag.set()
        sampler.join(timeout=10)
    time.sleep(15)   # останні скрейпи Prometheus
    samples = None
    for attempt in range(12):
        try:
            samples = prom_export.export(a.prom, t_begin - 40, time.time(), os.path.join(RES, f"{name}_prom.json"))
            break
        except Exception as exc:  # noqa: BLE001
            print(f"  Prometheus не відповів ({type(exc).__name__}), повтор {attempt + 1}/12", flush=True)
            time.sleep(10)
    info = {
        "mode": a.mode, "init_delay_s": a.init_delay, "profile": profile, "X1": x1, "XR": xr, "base_rps": base_rate,
        "jump_rps": jump, "t0_unix": t0, "prom_samples": samples,
        "hpa_describe": rs.kubectl("-n", "studies", "describe", "hpa", "web", check=False) if a.mode == "hpa" else None,
        "pods": rs.kubectl("-n", "studies", "get", "pods", "-l", "app=web", "-o", "wide"),
    }
    with open(os.path.join(RES, f"{name}_info.json"), "w", encoding="utf-8") as f:
        json.dump(info, f, indent=1, ensure_ascii=False)
    print("\nСегменти профілю:")
    for s in res["steps"]:
        print(f"  відрізок {s['segment']}: задано {s['offered_rps']}, досягнуто {s['achieved_mean_rps']}, "
              f"p99 клієнта {s['p99_ms']} мс, не 2xx {s['non_2xx']}, помилок зʼєднання {s['conn_errors']}")
    print(f"\nГотово: results/{name}.json, {name}_seconds.csv, {name}_prom.json, {name}_info.json")
    if a.mode == "hpa":
        print("Перед наступним прогоном із HPA нічого вручну робити не треба: скрипт сам прибере HPA і поверне 1 репліку.")


if __name__ == "__main__":
    main()
