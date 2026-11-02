"""Проміжний обробник метрик перевіряється напряму як ASGI-застосунок, без бази і мережі."""
import asyncio
from types import SimpleNamespace

import pytest
from prometheus_client import REGISTRY

from app.metrics import MetricsMiddleware

ROUTE = SimpleNamespace(path="/studies/{study_id}")


def sample(name, **labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


def make_app(status=200, fail=False):
    async def app(scope, receive, send):
        if fail:
            raise RuntimeError("boom")
        scope["route"] = ROUTE  # Starlette кладе маршрут у scope під час розбору шляху
        await send({"type": "http.response.start", "status": status, "headers": []})
        await send({"type": "http.response.body", "body": b""})
    return MetricsMiddleware(app)


def call(app, path="/studies/123", method="GET"):
    sent = []

    async def send(message):
        sent.append(message)

    async def receive():
        return {"type": "http.request"}

    scope = {"type": "http", "method": method, "path": path}
    asyncio.run(app(scope, receive, send))
    return sent


def test_counter_uses_route_template_not_concrete_id():
    before = sample("http_requests_total", method="GET", path="/studies/{study_id}", code="200")
    call(make_app(200), "/studies/123")
    call(make_app(200), "/studies/999")
    after = sample("http_requests_total", method="GET", path="/studies/{study_id}", code="200")
    assert after - before == 2
    assert sample("http_requests_total", method="GET", path="/studies/123", code="200") == 0


def test_histogram_count_and_buckets_in_seconds():
    labels = dict(method="GET", path="/studies/{study_id}")
    before = sample("http_request_duration_seconds_count", **labels)
    call(make_app(200))
    assert sample("http_request_duration_seconds_count", **labels) - before == 1
    # межа 1 мс має мітку le="0.001", межа 1024 мс — le="1.024", є і +Inf
    for le in ("0.001", "1.024", "+Inf"):
        assert REGISTRY.get_sample_value("http_request_duration_seconds_bucket", {**labels, "le": le}) is not None


def test_error_status_is_recorded():
    before = sample("http_requests_total", method="GET", path="/studies/{study_id}", code="404")
    call(make_app(404))
    assert sample("http_requests_total", method="GET", path="/studies/{study_id}", code="404") - before == 1


def test_unhandled_exception_counted_as_500_and_in_flight_returns_to_zero():
    before = sample("http_requests_total", method="GET", path="unmatched", code="500")
    with pytest.raises(RuntimeError):
        call(make_app(fail=True), "/nope")
    assert sample("http_requests_total", method="GET", path="unmatched", code="500") - before == 1
    assert sample("http_requests_in_flight") == 0


def test_metrics_endpoint_is_not_counted():
    call(make_app(200), "/metrics")
    assert REGISTRY.get_sample_value("http_requests_total", {"method": "GET", "path": "/metrics", "code": "200"}) is None


def test_unknown_method_gets_other_label():
    before = sample("http_requests_total", method="OTHER", path="/studies/{study_id}", code="200")
    call(make_app(200), method="BREW")
    assert sample("http_requests_total", method="OTHER", path="/studies/{study_id}", code="200") - before == 1
