"""
Shared logging helpers: get_logger and a simple timing decorator.
"""
from __future__ import annotations
import logging, time
from functools import wraps

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

def timeit(logger: logging.Logger, label: str):
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            start = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                dur = (time.time() - start) * 1000.0
                logger.info("%s completed in %.1f ms", label, dur)
        return wrapped
    return decorator
