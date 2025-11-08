import gc

import psutil


class MemoryMonitor:
    """Shared memory monitor with optional auto-GC.
    check() returns True if above threshold and triggers GC.
    """

    def __init__(self, threshold_percent: int = 80, auto_collect: bool = True):
        self.threshold = threshold_percent
        self.auto_collect = auto_collect

    def check(self) -> bool:
        memory = psutil.virtual_memory()
        if memory.percent > self.threshold:
            if self.auto_collect:
                gc.collect()
            return True
        return False

    # Remove legacy alias; prefer check()
