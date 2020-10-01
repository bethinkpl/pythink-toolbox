import logging
import os
import pathlib

import dotenv

import chronos


dotenv.load_dotenv(dotenv.find_dotenv())

# === External services ===
BIGQUERY_PLATFORM_DATASET_ID: str = os.getenv("BIGQUERY_PLATFORM_DATASET_ID", "")

# === Chronos specific ===
CHRONOS_PACKAGE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))

PROJECT_DIR = CHRONOS_PACKAGE_DIR.parents[1]


_SERVICE_NAME = chronos.__name__.upper()

LOG_LEVEL = os.getenv(f"{_SERVICE_NAME}_LOG_LEVEL", logging.getLevelName(logging.INFO))
SENTRY_DSN_API = os.getenv(f"{_SERVICE_NAME}_SENTRY_DSN_API")

MONGO_HOST = os.getenv(f"{_SERVICE_NAME}_MONGO_HOST", "")
MONGO_PORT = int(os.getenv(f"{_SERVICE_NAME}_MONGO_PORT", "0"))
MONGO_USERNAME = os.getenv(f"{_SERVICE_NAME}_MONGO_USER", "")
MONGO_PASSWORD = os.getenv(f"{_SERVICE_NAME}_MONGO_PASSWORD", "")
MONGO_DATABASE = os.getenv(f"{_SERVICE_NAME}_MONGO_DATABASE", "")
