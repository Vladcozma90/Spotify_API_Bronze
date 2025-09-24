from typing import Any
import logging
import re

logger = logging.getLogger(__name__)

def _ensure_dict(d: dict[str, Any], keys: list[str], ctx: str) -> None:
    if not isinstance(d, dict):
        logger.info("%s type is invalid. Got %s", ctx, type(d).__name__)
        raise ValueError(f"expected format for {ctx} is dict!")
    missing = [key for key in keys if key not in d]
    if missing:
        raise ValueError(f"missing keys in {ctx}: {missing}!")


def _ensure_list(x: list[dict], ctx: str) -> None:
    if not isinstance(x, list) or not x:
        raise ValueError(f"expected format for {ctx} is list!")
    

def slugify(text: str) -> str:
    slug = "".join(char.lower() if char.isalnum() else "-" for char in text)
    return re.sub("-+", "-", slug).strip("-")