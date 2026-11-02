# Правка scripts/lab4.py: (1) журнал воркера, що в CrashLoopBackOff, читається з вузла k3d (kubectl exec у такий
# под не працює, бо контейнера, що біжить, немає); (2) крок 9 можна запускати частково: --only f5_nohb f5_hb.
p = "scripts/lab4.py"
s = open(p, encoding="utf-8").read()

old = s[s.index("    def _exec_log(self, pod, cmd):"):s.index("    def log_grep_count(self, pod, pattern):")]
new = '''    def _node_exec(self, pod, cmd):
        """Читає журнал з тому emptyDir на вузлі k3d: працює і тоді, коли контейнер пода не запущено (пауза між
        перезапусками). Том пода лежить у /var/lib/kubelet/pods/<uid>/volumes/kubernetes.io~empty-dir/logs."""
        d = json.loads(self.kubectl("-n", "studies", "get", "pod", pod, "-o", "json"))
        path = f"/var/lib/kubelet/pods/{d['metadata']['uid']}/volumes/kubernetes.io~empty-dir/logs/worker.log"
        r = subprocess.run(["docker", "exec", d["spec"]["nodeName"], "sh", "-c",
                            cmd.replace("/var/log/worker/worker.log", path)],
                           encoding="utf-8", errors="replace", capture_output=True, timeout=60)
        if r.returncode != 0:
            raise RuntimeError(r.stderr.strip())
        return r.stdout

    unreadable = []

    def _exec_log(self, pod, cmd):
        """Читає журнал пода воркера: спершу kubectl exec, а якщо контейнер саме перезапускається (FAIL_RATE),
        то з вузла; кілька повторів із паузами."""
        err = None
        for _ in range(5):
            for reader in (lambda: self.kubectl("-n", "studies", "exec", pod, "--", "sh", "-c", cmd),
                           lambda: self._node_exec(pod, cmd)):
                try:
                    return reader()
                except Exception as exc:  # noqa: BLE001
                    err = exc
            time.sleep(3)
        print(f"   УВАГА: журнал пода {pod} не прочитано ({err}); його рядки не враховано", file=sys.stderr)
        self.unreadable.append(pod)
        return ""

'''
s = s.replace(old, new)

# часткові запуски кроку 9
a = s.index("    dur = T(300)\n    # A. FAIL_RATE")
b = s.index("    default_env(R)   # повертаємо типові значення")
block = '''    dur = T(300)
    only = set(a.only) if a.only else {"fail015", "f5_nohb", "f5_hb"}
    lease = t_cpu / 1000 / 2
    # A. FAIL_RATE = 0,15, L варіанта з heartbeat (без Ф5): порівняння X і розподілу attempts з X(R) без падінь
    if "fail015" in only:
        fail_run("step9_fail015", xr, LEASE_S, True, dur)
    # B, C. Сценарій Ф5: L = t_cpu / 2, спершу без heartbeat (B), далі з heartbeat (C); FAIL_RATE = 0,15 в обох
    if "f5_nohb" in only:
        fail_run("step9_f5_nohb", xr, lease, False, dur)
    if "f5_hb" in only:
        fail_run("step9_f5_hb", xr, lease, True, dur)
'''
s = s[:a] + block + s[b:]
s = s.replace('    ap.add_argument("--t-cpu-ms", type=float, default=None)\n',
              '    ap.add_argument("--t-cpu-ms", type=float, default=None)\n'
              '    ap.add_argument("--only", nargs="+", choices=["fail015", "f5_nohb", "f5_hb"], default=None, help="step9: які прогони")\n')
s = s.replace('"n_dup_results": dup, "zero_rows_log_lines": zero_rows,',
              '"n_dup_results": dup, "zero_rows_log_lines": zero_rows, "unreadable_log_pods": len(set(getattr(ENV, "unreadable", []))),')
open(p, "w", encoding="utf-8").write(s)
print("готово")
