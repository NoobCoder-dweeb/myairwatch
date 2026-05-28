import logging


def pytest_configure(config):
    """Prevent py4j from logging during interpreter shutdown which can
    cause "I/O operation on closed file" errors when pytest tears down.

    Attach a NullHandler to the py4j loggers and stop propagation so
    messages are dropped instead of being emitted to closed streams.
    """
    for name in ("py4j.clientserver", "py4j"):
        logger = logging.getLogger(name)
        # remove any existing handlers that might write to sys.stderr
        try:
            logger.handlers.clear()
        except Exception:
            pass
        logger.addHandler(logging.NullHandler())
        logger.propagate = False
