
import os

TEST_ENV = {
    "DB_HOST": "x", "DB_PORT": "5432", "DB_NAME": "x", "DB_USER": "x", "DB_PASSWORD": "x",
    "DB_POOL_MIN": "1", "DB_POOL_SIZE": "15", "DB_CONNECT_TIMEOUT": "1", "DB_COMMAND_TIMEOUT": "1",
    "HTTP_HOST": "127.0.0.1", "HTTP_PORT": "8000", "HTTP_WORKERS": "1",
    "DEFAULT_LIMIT": "20", "MAX_LIMIT": "100",
    "HISTOGRAM_BUCKETS_MS": "1,2,4,8,16,32,64,128,256,512,1024",
    "LOG_LEVEL": "info", "ACCESS_LOG": "false", "STARTUP_DELAY_SECONDS": "0",
}
for key, value in TEST_ENV.items():
    os.environ.setdefault(key, value)
