import logging
import os
from datetime import datetime


def configure_logger(module_name=__name__, log_level=logging.INFO):

    if module_name in logging.Logger.manager.loggerDict:
        return logging.getLogger(module_name).setLevel(log_level)

    logger = logging.getLogger(module_name)
    logger.setLevel(log_level)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if not os.path.exists("logs"):
        os.makedirs("logs")

    log_filename = (
        f"logs/log_{module_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    )
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
