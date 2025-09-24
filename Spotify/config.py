import os
import logging


def get_log():
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=LOG_LEVEL,
        format ="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s"
    )

def get_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"missing required env var: {key}")
    return val