import logging
import os

import dotenv

import chronos


dotenv.load_dotenv(dotenv.find_dotenv())

BIGQUERY_PLATFORM_DATASET_ID: str = os.getenv("BIGQUERY_PLATFORM_DATASET_ID", "")

_SERVICE_NAME = chronos.__name__.upper()

LOG_LEVEL = os.getenv(f"{_SERVICE_NAME}_LOG_LEVEL", logging.getLevelName(logging.INFO))
SENTRY_DSN_API = os.getenv(f"{_SERVICE_NAME}_SENTRY_DSN_API")

MONGO_HOST = os.getenv(f"{_SERVICE_NAME}_MONGO_HOST", "")
MONGO_PORT = int(os.getenv(f"{_SERVICE_NAME}_MONGO_PORT", "0"))
MONGO_USERNAME = os.getenv(f"{_SERVICE_NAME}_MONGO_USER", "")
MONGO_PASSWORD = os.getenv(f"{_SERVICE_NAME}_MONGO_PASSWORD", "")
MONGO_DATABASE = os.getenv(f"{_SERVICE_NAME}_MONGO_DATABASE", "")

HOST_API = os.getenv(f"{_SERVICE_NAME}_HOST_API")
