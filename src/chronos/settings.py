import logging
import os
import pathlib

import dotenv

ROOT_DIR = pathlib.Path(__file__).parents[2]
ENV_PATH = ROOT_DIR / ".env"

dotenv.load_dotenv(dotenv_path=ENV_PATH)

LOG_LEVEL = os.getenv("CHRONOS_LOG_LEVEL", logging.getLevelName(logging.INFO))

SENTRY_DSN_API = os.getenv("CHRONOS_SENTRY_DSN_API")

MONGO_HOST = os.getenv("CHRONOS_MONGO_HOST")
MONGO_PORT = int(os.getenv("CHRONOS_MONGO_PORT", "0"))
MONGO_USERNAME = os.getenv("CHRONOS_MONGO_USER")
MONGO_PASSWORD = os.getenv("CHRONOS_MONGO_PASSWORD")
MONGO_DATABASE = os.getenv("CHRONOS_MONGO_DATABASE")
