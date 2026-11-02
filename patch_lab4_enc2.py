# Правка scripts/lab4.py: вивід генератора задач у UTF-8 (щоб у терміналі не було «ромбиків»)
p = "scripts/lab4.py"
s = open(p, encoding="utf-8").read()
a = 'subprocess.Popen(cmd, cwd=ROOT, encoding="utf-8", errors="replace", stdout=subprocess.PIPE, stderr=subprocess.STDOUT)'
b = 'subprocess.Popen(cmd, cwd=ROOT, encoding="utf-8", errors="replace", stdout=subprocess.PIPE, stderr=subprocess.STDOUT,\n                         env={**os.environ, "PYTHONIOENCODING": "utf-8"})'
if a in s:
    s = s.replace(a, b)
elif b not in s:
    raise SystemExit("фрагмент не знайдено")
open(p, "w", encoding="utf-8").write(s)
print("готово")
