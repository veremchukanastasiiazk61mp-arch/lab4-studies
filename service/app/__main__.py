import contextlib
import signal
import sys
import threading
from collections.abc import Generator

import uvicorn

from .config import settings  # при некоректній конфігурації процес падає тут із зрозумілим повідомленням


class GracefulServer(uvicorn.Server):
    """Коректна зупинка за SIGTERM (ЛР2, крок 3).

    Стандартний uvicorn після зупинки повторно надсилає собі отриманий сигнал, і процес
    завершується зі статусом 128+15 = 143. Тут сигнал лише запускає штатну зупинку:
    сервер перестає приймати нові зʼєднання, дочікується запитів в обробці, lifespan
    закриває пул зʼєднань з базою, процес повертається з main() і виходить з кодом 0.
    """

    @contextlib.contextmanager
    def capture_signals(self) -> Generator[None, None, None]:
        if threading.current_thread() is not threading.main_thread():
            yield
            return
        previous = {sig: signal.signal(sig, self.handle_exit) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            yield
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
        # навмисно без signal.raise_signal(...): див. докстрінг класу


def main() -> int:
    config = uvicorn.Config(
        "app.main:app",
        host=settings.http_host,
        port=settings.http_port,
        log_level=settings.log_level.lower(),
        access_log=settings.access_log,
        loop="uvloop",
        http="httptools",
        server_header=False,
    )
    GracefulServer(config).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
