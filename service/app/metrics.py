"""Метрики Prometheus (ЛР2, крок 1).

Метрики реєструються один раз під час імпорту модуля, тобто під час запуску
сервісу, і оновлюються в проміжному обробнику MetricsMiddleware.

  http_requests_total{method,path,code}          лічильник запитів
  http_request_duration_seconds{method,path}     гістограма тривалості (секунди)
  http_requests_in_flight                        вимірювач запитів в обробці

Мітка path містить шаблон маршруту (/studies/{study_id}), а не конкретний
ідентифікатор, інакше кількість рядів необмежено росла б. Запити, що не
збіглись з жодним маршрутом, мають path="unmatched".
"""
import time

from prometheus_client import Counter, Gauge, Histogram
from starlette.routing import Match
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .config import settings

REQUESTS = Counter(
    "http_requests_total",
    "Кількість оброблених HTTP-запитів",
    ["method", "path", "code"],
)
DURATION = Histogram(
    "http_request_duration_seconds",
    "Тривалість обробки запиту сервісом, секунди",
    ["method", "path"],
    buckets=settings.histogram_buckets_s,  # +Inf prometheus_client додає сам
)
IN_FLIGHT = Gauge("http_requests_in_flight", "Кількість запитів в обробці")

KNOWN_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
EXCLUDED_PATHS = {"/metrics"}  # скрейп не має впливати на власні метрики


def route_template(scope: Scope) -> str:
    """Шаблон маршруту для мітки path."""
    route = scope.get("route")
    if route is not None:
        return route.path
    # Запасний шлях, якщо версія фреймворку не кладе route у scope
    app = scope.get("app")
    if app is not None:
        for r in app.router.routes:
            match, _ = r.matches(scope)
            if match == Match.FULL:
                return r.path
    return "unmatched"


class MetricsMiddleware:
    """Чистий ASGI-обробник: дешевший за BaseHTTPMiddleware, бо не копіює тіло відповіді."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] in EXCLUDED_PATHS:
            await self.app(scope, receive, send)
            return

        status = 500  # якщо виняток дійшов до ServerErrorMiddleware, клієнт отримає саме 500

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        IN_FLIGHT.inc()
        start = time.perf_counter()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = time.perf_counter() - start
            IN_FLIGHT.dec()
            method = scope["method"] if scope["method"] in KNOWN_METHODS else "OTHER"
            path = route_template(scope)
            REQUESTS.labels(method, path, str(status)).inc()
            DURATION.labels(method, path).observe(elapsed)


# ЛР4: довжина черги і кількість задач у обробці; значення оновлює фонове завдання сервісу
# запитом до бази кожні 5 секунд (див. main.py, _tasks_gauge_loop).
TASKS_QUEUED = Gauge("tasks_queued", "Кількість задач у статусі queued")
TASKS_RUNNING = Gauge("tasks_running", "Кількість задач у статусі running")
