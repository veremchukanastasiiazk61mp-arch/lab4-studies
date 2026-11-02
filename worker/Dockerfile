# syntax=docker/dockerfile:1
# ЛР4: образ воркера. Збирається так само, як образ сервісу (ЛР3): залежності в окремій стадії,
# у фінальному образі немає pip, конфігурації в образі немає: усе приходить зі змінних середовища.
FROM python:3.12-alpine AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --only-binary=:all: --prefix=/install -r requirements.txt

FROM python:3.12-alpine AS final
RUN pip uninstall -y pip setuptools wheel 2>/dev/null || true \
 && adduser -D -H -u 10001 app
COPY --from=builder /install /usr/local
WORKDIR /app
COPY app ./app
# каталог журналу: на його місце монтується том emptyDir, але права потрібні й без тому
RUN mkdir -p /var/log/worker && chown 10001 /var/log/worker
USER 10001
# Exec-форма: процес воркера є PID 1 і сам отримує SIGTERM; kill -STOP 1 / kill -CONT 1 (крок 5) теж діє на нього.
CMD ["python", "-m", "app"]
