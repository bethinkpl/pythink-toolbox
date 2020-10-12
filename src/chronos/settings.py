import logging
import os
import pathlib

import dotenv

dotenv.load_dotenv(dotenv.find_dotenv())

# === External services ===
BIGQUERY_PLATFORM_DATASET_ID: str = os.getenv("BIGQUERY_PLATFORM_DATASET_ID", "")

# === Chronos specific ===
CHRONOS_PACKAGE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))

PROJECT_DIR = CHRONOS_PACKAGE_DIR.parents[1]

LOG_LEVEL = os.getenv("CHRONOS_LOG_LEVEL", logging.getLevelName(logging.INFO))
SENTRY_DSN_API = os.getenv("CHRONOS_SENTRY_DSN_API")

MONGO_HOST = os.getenv("CHRONOS_MONGO_HOST", "")
MONGO_PORT = int(os.getenv("CHRONOS_MONGO_PORT", "0"))
MONGO_USERNAME = os.getenv("CHRONOS_MONGO_USER", "")
MONGO_PASSWORD = os.getenv("CHRONOS_MONGO_PASSWORD", "")
MONGO_DATABASE = os.getenv("CHRONOS_MONGO_DATABASE", "")

HOST_API = os.getenv("CHRONOS_HOST_API", "localhost")
