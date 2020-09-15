import logging
import os
import pathlib

import dotenv

ROOT_DIR = pathlib.Path(__file__).parents[2]
ENV_PATH = ROOT_DIR / ".env"

dotenv.load_dotenv(dotenv_path=ENV_PATH)

LOG_LEVEL = os.getenv("LOG_LEVEL_CHRONOS", logging.getLevelName(logging.INFO))

SENTRY_DSN_API = os.getenv("SENTRY_DSN_API_CHRONOS")
