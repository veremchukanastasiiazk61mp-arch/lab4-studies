# syntax=docker/dockerfile:1

# ---------- Стадія 1: збірка залежностей ----------
# Для всіх залежностей є готові musllinux-колеса, тому компілятор не потрібен
# навіть тут. Пакети ставляться в окремий префікс, який потім копіюється у фінальну стадію.
FROM python:3.12-alpine AS builder
WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --only-binary=:all: --prefix=/install -r requirements.txt

# ---------- Стадія 2: фінальний образ ----------
FROM python:3.12-alpine AS final
# Без pip і setuptools: у фінальній стадії немає засобів збірки та встановлення пакетів.
RUN pip uninstall -y pip setuptools wheel 2>/dev/null || true \
 && adduser -D -H -u 10001 app
COPY --from=builder /install /usr/local
WORKDIR /app
COPY app ./app
USER 10001
# Конфігурації в образі немає: жодних ENV, ARG і файлів конфігурації.
# Усе приходить із середовища контейнера (див. docker-compose.yml і .env.example).
# Exec-форма CMD (JSON-масив): процес сервісу є PID 1 і сам отримує SIGTERM від docker stop.
# Shell-форма запустила б /bin/sh -c, який сигнал не передає. Обробник SIGTERM — у app/__main__.py.
CMD ["python", "-m", "app"]
