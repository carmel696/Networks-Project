import os
from datetime import datetime


class Proc_Logger:
    def __init__(self, path):
        self.path = path

    def append(self, message):
        """Write to the peer's existing log without creating a missing file."""
        descriptor = os.open(self.path, os.O_WRONLY | os.O_APPEND)
        with os.fdopen(descriptor, "a", encoding="utf-8") as log:
            timestamp = datetime.now().isoformat(sep=" ", timespec="seconds")
            log.write(f"{timestamp}: {message}\n")
