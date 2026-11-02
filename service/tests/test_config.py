"""Конфігурація ЛР2: DB_POOL_SIZE, бакети гістограми, один процес, POD_NAME."""
import contextlib
import os
import socket

import pytest

from app import config


@contextlib.contextmanager
def env(**changes):
    """Тимчасово змінює змінні середовища; None видаляє змінну."""
    saved = {k: os.environ.get(k) for k in changes}
    try:
        for k, v in changes.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_pool_size_and_buckets_in_seconds():
    s = config.load_settings()
    assert s.db_pool_size == 15
    assert s.histogram_buckets_s[0] == 0.001
    assert s.histogram_buckets_s[-1] == 1.024
    assert len(s.histogram_buckets_s) == 11


def test_pool_size_is_required():
    with env(DB_POOL_SIZE=None):
        with pytest.raises(config.ConfigError, match="DB_POOL_SIZE"):
            config.load_settings()


@pytest.mark.parametrize("value", ["1,1,2", "4,2", "0,1", "a,b", "-1,2"])
def test_bad_buckets_rejected(value):
    with env(HISTOGRAM_BUCKETS_MS=value):
        with pytest.raises(config.ConfigError, match="HISTOGRAM_BUCKETS_MS"):
            config.load_settings()


def test_single_process_required():
    with env(HTTP_WORKERS="2"):
        with pytest.raises(config.ConfigError, match="HTTP_WORKERS"):
            config.load_settings()


def test_pod_name_falls_back_to_hostname():
    with env(POD_NAME=None):
        assert config.load_settings().pod_name == socket.gethostname()
    with env(POD_NAME="web-7"):
        assert config.load_settings().pod_name == "web-7"


def test_startup_delay_required_and_not_negative():
    """ЛР3, Д6: STARTUP_DELAY_SECONDS обовʼязкова, 0 дозволено, відʼємна ні."""
    with env(STARTUP_DELAY_SECONDS="15"):
        assert config.load_settings().startup_delay_s == 15.0
    with env(STARTUP_DELAY_SECONDS="0"):
        assert config.load_settings().startup_delay_s == 0.0
    with env(STARTUP_DELAY_SECONDS="-1"):
        with pytest.raises(config.ConfigError):
            config.load_settings()
    with env(STARTUP_DELAY_SECONDS=None):
        with pytest.raises(config.ConfigError):
            config.load_settings()
