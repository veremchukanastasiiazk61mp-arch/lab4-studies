
import os
import socket
from dataclasses import dataclass


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class Settings:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    db_pool_min: int
    db_pool_size: int  # ЛР2: найбільша кількість відкритих зʼєднань екземпляра (без overflow)
    db_connect_timeout: float
    db_command_timeout: float
    http_host: str
    http_port: int
    http_workers: int
    default_limit: int
    max_limit: int
    log_level: str
    access_log: bool
    histogram_buckets_s: tuple[float, ...]  # ЛР2: верхні межі бакетів гістограми, секунди
    pod_name: str  # ЛР2: імʼя екземпляра для заголовка X-Pod
    startup_delay_s: float  # ЛР3, Д6: тривалість початкової ініціалізації після запуску, с (0 = без неї)


def load_settings() -> Settings:
    problems: list[str] = []

    def raw(name: str) -> str | None:
        value = os.environ.get(name)
        if value is None or value == "":
            problems.append(f"{name}: змінну не задано")
            return None
        return value

    def as_int(name: str, minimum: int) -> int:
        value = raw(name)
        if value is None:
            return 0
        try:
            number = int(value)
        except ValueError:
            problems.append(f"{name}: '{value}' не є цілим числом")
            return 0
        if number < minimum:
            problems.append(f"{name}: має бути не менше {minimum}")
        return number

    def as_float(name: str) -> float:
        value = raw(name)
        if value is None:
            return 0.0
        try:
            number = float(value)
        except ValueError:
            problems.append(f"{name}: '{value}' не є числом")
            return 0.0
        if number <= 0:
            problems.append(f"{name}: має бути більше нуля")
        return number

    def as_nonneg_float(name: str) -> float:
        value = raw(name)
        if value is None:
            return 0.0
        try:
            number = float(value)
        except ValueError:
            problems.append(f"{name}: '{value}' не є числом")
            return 0.0
        if number < 0:
            problems.append(f"{name}: не може бути відʼємним")
        return number

    def as_bool(name: str) -> bool:
        value = raw(name)
        if value is None:
            return False
        if value.lower() not in ("true", "false"):
            problems.append(f"{name}: допустимо лише true або false")
        return value.lower() == "true"

    def as_str(name: str) -> str:
        return raw(name) or ""

    def as_buckets_ms(name: str) -> tuple[float, ...]:
        """Межі бакетів у мілісекундах через кому, строго зростаючі; повертає секунди."""
        value = raw(name)
        if value is None:
            return ()
        try:
            ms = [float(x) for x in value.split(",")]
        except ValueError:
            problems.append(f"{name}: очікується перелік чисел через кому, а не '{value}'")
            return ()
        if any(m <= 0 for m in ms) or any(a >= b for a, b in zip(ms, ms[1:])):
            problems.append(f"{name}: межі мають бути додатними і строго зростати")
        return tuple(m / 1000 for m in ms)

    s = Settings(
        db_host=as_str("DB_HOST"),
        db_port=as_int("DB_PORT", 1),
        db_name=as_str("DB_NAME"),
        db_user=as_str("DB_USER"),
        db_password=as_str("DB_PASSWORD"),
        db_pool_min=as_int("DB_POOL_MIN", 1),
        db_pool_size=as_int("DB_POOL_SIZE", 1),
        db_connect_timeout=as_float("DB_CONNECT_TIMEOUT"),
        db_command_timeout=as_float("DB_COMMAND_TIMEOUT"),
        http_host=as_str("HTTP_HOST"),
        http_port=as_int("HTTP_PORT", 1),
        http_workers=as_int("HTTP_WORKERS", 1),
        default_limit=as_int("DEFAULT_LIMIT", 1),
        max_limit=as_int("MAX_LIMIT", 1),
        log_level=as_str("LOG_LEVEL"),
        access_log=as_bool("ACCESS_LOG"),
        histogram_buckets_s=as_buckets_ms("HISTOGRAM_BUCKETS_MS"),
        startup_delay_s=as_nonneg_float("STARTUP_DELAY_SECONDS"),

        pod_name=os.environ.get("POD_NAME") or socket.gethostname(),
    )

    if not problems:
        if s.db_pool_min > s.db_pool_size:
            problems.append("DB_POOL_MIN не може перевищувати DB_POOL_SIZE")
        if s.http_workers != 1:
            # prometheus_client без multiprocess-режиму дає коректні метрики лише в одному процесі
            problems.append("HTTP_WORKERS: ЛР2 вимагає рівно один процес у контейнері")
        if s.default_limit > s.max_limit:
            problems.append("DEFAULT_LIMIT не може перевищувати MAX_LIMIT")
        if s.log_level.lower() not in ("debug", "info", "warning", "error"):
            problems.append("LOG_LEVEL: debug, info, warning або error")

    if problems:
        raise ConfigError("Некоректна конфігурація:\n  " + "\n  ".join(problems))
    return s


settings = load_settings()
