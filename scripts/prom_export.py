
import argparse
import json
import os
import time
import urllib.parse
import urllib.request


SELECTORS = {
    # метрики самого сервісу (кожен под web окремим рядом, мітка pod)
    "bucket": 'http_request_duration_seconds_bucket{job="web",path="/studies/{study_id}"}',
    "requests": 'http_requests_total{job="web"}',
    "in_flight": 'http_requests_in_flight{job="web"}',
    "up": 'up{job="web"}',
    # cAdvisor через kubelet: використання CPU і дроселювання (квота CFS)
    "cpu_usage": 'container_cpu_usage_seconds_total{namespace="studies",container="web"}',
    "cfs_periods": 'container_cpu_cfs_periods_total{namespace="studies",container="web"}',
    "cfs_throttled": 'container_cpu_cfs_throttled_periods_total{namespace="studies",container="web"}',
    "mem_working_set": 'container_memory_working_set_bytes{namespace="studies",container="web"}',
    # kube-state-metrics: готові репліки, HPA, requests для обчислення відносного використання
    "replicas_ready": 'kube_deployment_status_replicas_ready{namespace="studies",deployment="web"}',
    "replicas": 'kube_deployment_status_replicas{namespace="studies",deployment="web"}',
    "hpa_desired": 'kube_horizontalpodautoscaler_status_desired_replicas{namespace="studies"}',
    "hpa_current": 'kube_horizontalpodautoscaler_status_current_replicas{namespace="studies"}',
    "pod_ready": 'kube_pod_status_ready{namespace="studies",condition="true"}',
    "pod_created": 'kube_pod_created{namespace="studies"}',
    # М2 (варіант 5): postgres-exporter
    "pg_conn": "pg_conn_count",
    "pg_xact_commit": "pg_stat_database_xact_commit",
    "pg_numbackends": "pg_stat_database_numbackends",
}


def api(prom, path, params):
    url = f"{prom.rstrip('/')}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError(f"Prometheus: {body}")
    return body["data"]


def prom_clock_offset(prom):
    """host_time − prometheus_time, с. Годинник віртуальної машини Docker може відставати."""
    before = time.time()
    data = api(prom, "/api/v1/query", {"query": "time()"})
    after = time.time()
    return round((before + after) / 2 - float(data["result"][1]), 3)


def export(prom, host_start, host_end, out_path, extra=None):
    """host_start/host_end — unix-час за годинником хоста; у файлі часові мітки вибірок
    залишаються за годинником Prometheus, а зсув clock_offset_s (хост − Prometheus) записано поруч."""
    offset = prom_clock_offset(prom)
    start, end = host_start - offset, host_end - offset
    duration = int(end - start) + 1
    result = {"host_start": host_start, "host_end": host_end, "prom": prom, "clock_offset_s": offset, "series": {}}
    for name, sel in SELECTORS.items():
        data = api(prom, "/api/v1/query", {"query": f"{sel}[{duration}s]", "time": end})
        result["series"][name] = [
            {"metric": s["metric"], "values": [[float(t), float(v)] for t, v in s["values"]]}
            for s in data["result"]
        ]
    if extra:
        result.update(extra)
    with open(out_path, "w") as f:
        json.dump(result, f)
    n = sum(len(s["values"]) for v in result["series"].values() for s in v)
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", default="http://localhost:9090")
    ap.add_argument("--start", type=float, help="unix-час початку (за годинником хоста)")
    ap.add_argument("--end", type=float)
    ap.add_argument("--run", help="імʼя прогону (напр. step_n3): межі часу беруться з results/<імʼя>.json і _seconds.csv")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    if a.run:
        base = os.path.join(os.path.dirname(__file__), "..", "results", a.run)
        with open(base + ".json") as f:
            t0 = json.load(f)["config"]["t0_unix"]
        with open(base + "_seconds.csv") as f:
            last = sum(1 for _ in f) - 1
        a.start, a.end = t0 - 20, t0 + last + 15
    if a.start is None or a.end is None:
        raise SystemExit("вкажіть --run або --start і --end")
    n = export(a.prom, a.start, a.end, a.out)
    print(f"{a.out}: {n} вибірок")


if __name__ == "__main__":
    main()
