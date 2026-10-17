import logging
import logging.config
from copy import deepcopy
from pathlib import Path

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG

from data_infra.utils import load_config

PROJECT_ROOT = Path(__file__).resolve().parents[2]
try:
    logging_config = load_config(PROJECT_ROOT / "config" / "logging.yml")
    logging.config.dictConfig(logging_config)
except FileNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger(__name__).warning("Coul not load logging config, usign default")


AIRFLOW_LOGGING_CONFIG = deepcopy(DEFAULT_LOGGING_CONFIG)
