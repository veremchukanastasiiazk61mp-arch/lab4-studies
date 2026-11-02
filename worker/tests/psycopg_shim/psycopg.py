"""Підміна psycopg ТІЛЬКИ для локальної перевірки без доступу до PyPI: виконує SQL через постійний процес psql.
У кластері використовується справжній psycopg 3 (worker/requirements.txt). Підтримано рівно те, що використовує воркер:
connect(...), Connection.execute(sql, params) із параметрами %s, .closed, .close(), cursor.description / fetchall()."""
import subprocess

_END = "<<END-OF-STATEMENT>>"


class Error(Exception):
    pass


class OperationalError(Error):
    pass


def _lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


class Cursor:
    def __init__(self, rows, has_rows):
        self._rows = rows
        self.description = True if has_rows else None

    def fetchall(self):
        return self._rows


class Connection:
    def __init__(self, host, port, dbname, user, application_name=""):
        self.proc = subprocess.Popen(
            ["psql", "-X", "-q", "-A", "-t", "-F", "\x1f", "-h", host, "-p", str(port), "-U", user, "-d", dbname],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
            env={"PGAPPNAME": application_name, "PATH": "/usr/bin:/bin"})
        self.closed = False
        self.execute("SELECT 1")

    def execute(self, sql, params=()):
        if self.closed or self.proc.poll() is not None:
            self.closed = True
            raise OperationalError("з'єднання закрито")
        parts = sql.split("%s")
        assert len(parts) == len(params) + 1, (sql, params)
        text = parts[0] + "".join(_lit(p) + parts[i + 1] for i, p in enumerate(params))
        try:
            self.proc.stdin.write(text + ";\n\\echo " + _END + "\n")
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            self.closed = True
            raise OperationalError(str(exc))
        out, err = [], None
        while True:
            line = self.proc.stdout.readline()
            if line == "":
                self.closed = True
                raise OperationalError(err or "з'єднання розірвано")
            line = line.rstrip("\n")
            if line == _END:
                break
            if "ERROR:" in line or "FATAL:" in line:
                err = line
            else:
                out.append(line)
        if err:
            raise Error(err)
        up = sql.lstrip().upper()
        has_rows = up.startswith("SELECT") or "RETURNING" in up
        return Cursor([tuple(l.split("\x1f")) for l in out if l != ""], has_rows)

    def close(self):
        if not self.closed:
            self.closed = True
            try:
                self.proc.stdin.close()
                self.proc.wait(timeout=2)
            except Exception:  # noqa: BLE001
                self.proc.kill()


def connect(host, port, dbname, user, password=None, autocommit=True, connect_timeout=5, application_name=""):
    return Connection(host, port, dbname, user, application_name)
