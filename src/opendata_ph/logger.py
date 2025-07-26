import logging
from logging import Logger


def create_logger(logger_name: str, level: int = logging.INFO) -> Logger:

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)

    formatter = logging.Formatter("%(asctime)s %(name)s (%(levelname)s) - %(message)s")

    ch.setFormatter(formatter)

    logger.addHandler(ch)

    return logger
