
import os
from dataclasses import dataclass


class ConfigError(RuntimeError):
    pass


CLAIM_MODES = ("naive", "for_update", "skip_locked")


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    web_url: str            # адреса сервісу web, звідки береться запис (крок 1 задачі)
    worker_id: str          # у Kubernetes заповнюється через Downward API з імені пода
    lease_s: float          # L: тривалість lease, с
    poll_s: float           # T_poll: інтервал опитування порожньої черги, с
    fail_rate: float        # FAIL_RATE: імовірність падіння між обробкою і записом результату
    claim_mode: str         # naive | for_update | skip_locked (кроки 3 і 4)
    heartbeat: bool         # false лише у прогоні Ф5 «без heartbeat»
    slice_size: int         # S
    q_lo: float
    q_hi: float
    buckets_s: tuple[float, ...]
    metrics_port: int
    grace_s: float
    http_timeout_s: float
    log_level: str
    log_file: str           # дублює журнал у файл на томі emptyDir: файл переживає перезапуски контейнера в поді


def load_settings() -> Settings:
    problems: list[str] = []

    def raw(name):
        v = os.environ.get(name)
        if v is None or v == "":
            problems.append(f"{name}: змінну не задано")
            return None
        return v

    def num(name, conv, lo=None, hi=None, strict_lo=False):
        v = raw(name)
        if v is None:
            return conv(0)
        try:
            x = conv(v)
        except ValueError:
            problems.append(f"{name}: '{v}' не є числом")
            return conv(0)
        if lo is not None and (x <= lo if strict_lo else x < lo):
            problems.append(f"{name}: має бути {'більше' if strict_lo else 'не менше'} {lo}")
        if hi is not None and x > hi:
            problems.append(f"{name}: має бути не більше {hi}")
        return x

    def text(name):
        return raw(name) or ""

    def boolean(name):
        v = raw(name)
        if v is not None and v.lower() not in ("true", "false"):
            problems.append(f"{name}: допустимо лише true або false")
        return (v or "").lower() == "true"

    def buckets(name):
        v = raw(name)
        if v is None:
            return ()
        try:
            ms = [float(t) for t in v.split(",")]
        except ValueError:
            problems.append(f"{name}: очікується перелік чисел через кому")
            return ()
        if any(m <= 0 for m in ms) or any(a >= b for a, b in zip(ms, ms[1:])):
            problems.append(f"{name}: межі мають бути додатними і строго зростати")
        return tuple(m / 1000 for m in ms)

    s = Settings(
        db_host=text("DB_HOST"), db_port=num("DB_PORT", int, 1), db_name=text("DB_NAME"),
        db_user=text("DB_USER"), db_password=text("DB_PASSWORD"),
        web_url=text("WEB_URL").rstrip("/"), worker_id=text("WORKER_ID"),
        lease_s=num("LEASE_SECONDS", float, 0, strict_lo=True),
        poll_s=num("POLL_INTERVAL_MS", float, 0, strict_lo=True) / 1000,
        fail_rate=num("FAIL_RATE", float, 0, 1),
        claim_mode=text("CLAIM_MODE"), heartbeat=boolean("HEARTBEAT"),
        slice_size=num("SLICE_SIZE", int, 1),
        q_lo=num("Q_LO", float, 0, 1), q_hi=num("Q_HI", float, 0, 1),
        buckets_s=buckets("HISTOGRAM_BUCKETS_MS"),
        metrics_port=num("METRICS_PORT", int, 1),
        grace_s=num("GRACE_SECONDS", float, 0, strict_lo=True),
        http_timeout_s=num("HTTP_TIMEOUT_SECONDS", float, 0, strict_lo=True),
        log_level=text("LOG_LEVEL"), log_file=text("LOG_FILE"),
    )
    if not problems:
        if s.claim_mode not in CLAIM_MODES:
            problems.append(f"CLAIM_MODE: {', '.join(CLAIM_MODES)}")
        if s.q_lo >= s.q_hi:
            problems.append("Q_LO має бути менше за Q_HI")
        if s.log_level.lower() not in ("debug", "info", "warning", "error"):
            problems.append("LOG_LEVEL: debug, info, warning або error")
    if problems:
        raise ConfigError("Некоректна конфігурація воркера:\n  " + "\n  ".join(problems))
    return s
