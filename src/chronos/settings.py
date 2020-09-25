import os
import pathlib

import dotenv

import chronos

ROOT_DIR = pathlib.Path(__file__).parents[2]
ENV_PATH = ROOT_DIR / ".env"

dotenv.load_dotenv(dotenv_path=ENV_PATH)

BIGQUERY_PLATFORM_DATASET_ID: str = os.getenv("BIGQUERY_PLATFORM_DATASET_ID", "")

prefix = chronos.__name__.upper()

MONGO_HOST = os.getenv(f"{prefix}_MONGO_HOST", "")
MONGO_PORT = int(os.getenv(f"{prefix}_MONGO_PORT", "0"))
MONGO_USERNAME = os.getenv(f"{prefix}_MONGO_USER", "")
MONGO_PASSWORD = os.getenv(f"{prefix}_MONGO_PASSWORD", "")
MONGO_DATABASE = os.getenv(f"{prefix}_MONGO_DATABASE", "")
