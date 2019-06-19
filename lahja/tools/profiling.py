import contextlib
import cProfile
from pathlib import Path
from typing import (
    Iterator,
)


@contextlib.contextmanager
def profiler(path: Path) -> Iterator[None]:
    pr = cProfile.Profile()
    pr.enable()
    try:
        yield
    finally:
        pr.disable()
        pr.dump_stats(str(path))
