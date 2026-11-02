# Правка scripts/lab4.py: читання журналів воркерів із повторами. Під час кроку 9 (FAIL_RATE = 0,15) контейнери
# падають і перезапускаються, і kubectl exec у момент перезапуску відповідає «container not found».
p = "scripts/lab4.py"
s = open(p, encoding="utf-8").read()
old = '''    def log_grep_count(self, pod, pattern):
        out = self.kubectl("-n", "studies", "exec", pod, "--", "sh", "-c",
                           f"grep -cF -- {shlex.quote(pattern)} /var/log/worker/worker.log || true")
        return int(out.strip() or 0)

    def log_grep(self, pod, pattern):
        out = self.kubectl("-n", "studies", "exec", pod, "--", "sh", "-c",
                           f"grep -F -- {shlex.quote(pattern)} /var/log/worker/worker.log || true")
        return out.splitlines()
'''
new = '''    def _exec_log(self, pod, cmd):
        """kubectl exec у под воркера з повторами: контейнер може саме перезапускатися (FAIL_RATE)."""
        err = None
        for _ in range(8):
            try:
                return self.kubectl("-n", "studies", "exec", pod, "--", "sh", "-c", cmd)
            except RuntimeError as exc:
                err = exc
                time.sleep(3)
        print(f"   УВАГА: журнал пода {pod} не прочитано ({err}); його рядки не враховано", file=sys.stderr)
        return ""

    def log_grep_count(self, pod, pattern):
        out = self._exec_log(pod, f"grep -cF -- {shlex.quote(pattern)} /var/log/worker/worker.log || true")
        return int(out.strip() or 0)

    def log_grep(self, pod, pattern):
        out = self._exec_log(pod, f"grep -F -- {shlex.quote(pattern)} /var/log/worker/worker.log || true")
        return out.splitlines()
'''
if old in s:
    s = s.replace(old, new)
elif "_exec_log" not in s:
    raise SystemExit("фрагмент не знайдено")
open(p, "w", encoding="utf-8").write(s)
print("готово")
