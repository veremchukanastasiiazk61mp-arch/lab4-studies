"""Мінімальна підміна prometheus_client ТІЛЬКИ для локальної перевірки без PyPI (формат експозиції текстовий,
як у справжньої бібліотеки). У кластері використовується справжній prometheus_client."""
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_REG = []
_LOCK = threading.Lock()


class _Metric:
    def __init__(self, name, doc, labelnames=(), buckets=None):
        self.name, self.doc, self.labelnames = name, doc, tuple(labelnames)
        self.buckets = list(buckets or []) + [float("inf")]
        self.v = {}
        _REG.append(self)

    def labels(self, *vals):
        key = tuple(vals)
        with _LOCK:
            self.v.setdefault(key, self._new())
        return _Child(self, key)

    def _new(self):
        return [0.0]

    def _fmt(self, key, extra=None):
        d = dict(zip(self.labelnames, key))
        if extra:
            d.update(extra)
        return "{" + ",".join(f'{k}="{v}"' for k, v in d.items()) + "}" if d else ""


class _Child:
    def __init__(self, m, key):
        self.m, self.key = m, key

    def inc(self, a=1):
        with _LOCK:
            self.m.v[self.key][0] += a

    def dec(self, a=1):
        self.inc(-a)

    def observe(self, x):
        with _LOCK:
            st = self.m.v[self.key]
            st[0] += 1
            st[1] += x
            for i, b in enumerate(self.m.buckets):
                if x <= b:
                    st[2 + i] += 1


class Counter(_Metric):
    def inc(self, a=1):
        self.labels().inc(a)

    def render(self):
        return [f"{self.name if self.name.endswith('_total') else self.name + '_total'}{self._fmt(k)} {st[0]}" for k, st in self.v.items()]


class Gauge(_Metric):
    def inc(self, a=1):
        self.labels().inc(a)

    def dec(self, a=1):
        self.labels().inc(-a)

    def set(self, x):
        with _LOCK:
            self.v.setdefault((), [0.0])[0] = x

    def render(self):
        return [f"{self.name}{self._fmt(k)} {st[0]}" for k, st in self.v.items()]


class Histogram(_Metric):
    def _new(self):
        return [0, 0.0] + [0] * len(self.buckets)

    def observe(self, x):
        self.labels().observe(x)

    def render(self):
        out = []
        for k, st in self.v.items():
            for i, b in enumerate(self.buckets):
                le = "+Inf" if b == float("inf") else repr(b)
                out.append(f"{self.name}_bucket{self._fmt(k, {'le': le})} {st[2 + i]}")
            out += [f"{self.name}_count{self._fmt(k)} {st[0]}", f"{self.name}_sum{self._fmt(k)} {st[1]}"]
        return out


CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"


def generate_latest():
    with _LOCK:
        lines = [l for m in _REG for l in m.render()]
    return ("\n".join(lines) + "\n").encode()


def start_http_server(port):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            b = generate_latest()
            self.send_response(200); self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)

        def log_message(self, *a):
            pass
    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
