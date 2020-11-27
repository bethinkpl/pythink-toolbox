import logging
import os
import pathlib
from typing import Final

import dotenv

CHRONOS_PACKAGE_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))

PROJECT_DIR = CHRONOS_PACKAGE_DIR.parents[1]

dotenv.load_dotenv(PROJECT_DIR / ".env")


# === CHRONOS SPECIFIC ===

ACTIVITY_SESSIONS_GENERATION_CHUNK_SIZE: Final[int] = 30

# --- Storage ---
MONGO_HOST = os.getenv("CHRONOS_MONGO_HOST", "")
MONGO_PORT = int(os.getenv("CHRONOS_MONGO_PORT", "0"))
MONGO_USERNAME = os.getenv("CHRONOS_MONGO_USER", "")
MONGO_PASSWORD = os.getenv("CHRONOS_MONGO_PASSWORD", "")
MONGO_DATABASE = os.getenv("CHRONOS_MONGO_DATABASE", "")

# --- Sentry ---
LOG_LEVEL = os.getenv("CHRONOS_LOG_LEVEL", logging.getLevelName(logging.INFO))
SENTRY_DSN_API = os.getenv("CHRONOS_SENTRY_DSN_API")

# === EXTERNAL SERVICES ===
BIGQUERY_PLATFORM_DATASET_ID: str = os.getenv("BIGQUERY_PLATFORM_DATASET_ID", "")
