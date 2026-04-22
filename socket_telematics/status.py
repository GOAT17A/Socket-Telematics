from __future__ import annotations

import sys
import threading
import time
from collections.abc import Callable


class StatusLine(threading.Thread):
    """Continuously prints a single-line status so active clients stay visible.

    Uses carriage-return updates when stdout is a TTY.
    """

    def __init__(self, *, get_status: Callable[[], str], shutdown: threading.Event, refresh_s: float = 1.0) -> None:
        super().__init__(daemon=True, name="status")
        self._get_status = get_status
        self._shutdown = shutdown
        self._refresh_s = refresh_s

    def run(self) -> None:
        is_tty = sys.stdout.isatty()

        while not self._shutdown.is_set():
            try:
                status = self._get_status()
            except Exception:  # noqa: BLE001
                status = "STATUS: unavailable"

            if is_tty:
                sys.stdout.write("\r" + status[:200].ljust(200))
                sys.stdout.flush()
            else:
                print(status)

            time.sleep(self._refresh_s)

        if is_tty:
            sys.stdout.write("\n")
            sys.stdout.flush()
