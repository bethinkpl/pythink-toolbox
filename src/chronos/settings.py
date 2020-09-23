import logging
import os
import pathlib

import dotenv

ROOT_DIR = pathlib.Path(__file__).parents[2]
ENV_PATH = ROOT_DIR / ".env"

dotenv.load_dotenv(dotenv_path=ENV_PATH)

LOG_LEVEL = os.getenv("LOG_LEVEL_CHRONOS", logging.getLevelName(logging.INFO))

SENTRY_DSN_API = os.getenv("SENTRY_DSN_API_CHRONOS")

MONGO_HOST = os.getenv("MONGO_HOST_CHRONOS")
MONGO_PORT = int(os.getenv("MONGO_PORT_CHRONOS", "0"))
MONGO_USERNAME = os.getenv("MONGO_USER_CHRONOS")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD_CHRONOS")
MONGO_DATABASE = os.getenv("MONGO_DATABASE_CHRONOS")
