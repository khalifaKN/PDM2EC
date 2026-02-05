from logging.handlers import TimedRotatingFileHandler
import logging
import os
import sys

def get_logger(name='logger', log_level='INFO'):
    logger = logging.getLogger(name)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s [%(name)s] '
        '%(filename)s:%(lineno)d %(funcName)s() - %(message)s'
    )

    # ---- INFO + DEBUG to STDOUT ----
    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(lambda r: r.levelno < logging.ERROR)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    # ---- ERROR + WARNING to STDERR ----
    stderr_handler = logging.StreamHandler(stream=sys.stderr)
    stderr_handler.setLevel(logging.ERROR)
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)

    # Directory for logs (relative to this file)
    log_dir = os.path.join(os.environ.get("TEMP", r"C:\temp"), "pdm2ec_logs")
    os.makedirs(log_dir, exist_ok=True)

    log_path = os.path.join(log_dir, "pdm2ec_log.log")

    # Rotate every hour and keep 48 hours of history
    file_handler = TimedRotatingFileHandler(
        log_path,
        when='H',
        interval=1,
        backupCount=48,
        encoding='utf-8',
        delay=True
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger