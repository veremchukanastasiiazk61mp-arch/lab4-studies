# Правка scripts/lab4.py: кодування UTF-8 для виводу kubectl/docker (на Windows кирилиця читалась у cp1251)
p = "scripts/lab4.py"
s = open(p, encoding="utf-8").read()
reps = [
    ('subprocess.run([*self.kc, *args], cwd=ROOT, text=True, capture_output=True, input=input, timeout=timeout)',
     'subprocess.run([*self.kc, *args], cwd=ROOT, encoding="utf-8", errors="replace", capture_output=True, input=input, timeout=timeout)'),
    ('subprocess.run(["docker", "exec", node, "sh", "-c", script], text=True, capture_output=True, timeout=60)',
     'subprocess.run(["docker", "exec", node, "sh", "-c", script], encoding="utf-8", errors="replace", capture_output=True, timeout=60)'),
    ('subprocess.Popen(cmd, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)',
     'subprocess.Popen(cmd, cwd=ROOT, encoding="utf-8", errors="replace", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)'),
    ('re.search(r"задача (\\d+) виконано", line)', 're.search(r"\\[task_done\\]\\D*(\\d+)", line)'),
]
for a, b in reps:
    if a in s:
        s = s.replace(a, b)
    elif b not in s:
        raise SystemExit("не знайдено фрагмент: " + a[:60])
open(p, "w", encoding="utf-8").write(s)
print("готово")
