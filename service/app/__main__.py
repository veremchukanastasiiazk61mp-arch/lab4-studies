import contextlib
import signal
import sys
import threading
from collections.abc import Generator

import uvicorn

from .config import settings


class GracefulServer(uvicorn.Server):


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
