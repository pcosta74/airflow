import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from urllib.parse import quote
from uuid import uuid4

from yaml import safe_load

logger = logging.getLogger(__name__)


def load_config(config_path: str | Path) -> dict:
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with open(config_path, "r") as file:
        return safe_load(file)


def safe_tofloat(val, default_value: float) -> float:
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        logger.warning(
            f"Invalid non-numeric value '{val}' detected. "
            f"Falling back to default layout size: {default_value}"
        )
        return default_value


def safe_toint(val, default_value: int) -> int:
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        logger.warning(
            f"Invalid non-numeric value '{val}' detected. "
            f"Falling back to default layout size: {default_value}"
        )
        return default_value


def run_id_partition(run_id: str | None) -> str:
    """Return a filesystem-safe partition name unique to an Airflow run."""
    return f"run_id={quote(run_id or uuid4().hex, safe='')}"


@contextmanager
def atomic_output_path(output_path: Path) -> Generator[Path, None, None]:
    """Write beside the destination, then atomically publish the complete file."""
    temp_path = output_path.with_name(f".{output_path.name}.{uuid4().hex}.tmp")
    try:
        yield temp_path
        os.replace(temp_path, output_path)
    finally:
        temp_path.unlink(missing_ok=True)
