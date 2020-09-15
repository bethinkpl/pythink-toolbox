import os
import pathlib
from typing import Optional

import dotenv
from pymongo.collection import Collection
from pymongo import MongoClient

ROOT_DIR = pathlib.Path(__file__).parents[2]
ENV_PATH = ROOT_DIR / ".env"

dotenv.load_dotenv(dotenv_path=ENV_PATH)

BIGQUERY_PLATFORM_DATASET_ID: str = os.getenv("BIGQUERY_PLATFORM_DATASET_ID", "")

MONGO_HOST = os.getenv("MONGO_HOST_CHRONOS", "")
MONGO_PORT = int(os.getenv("MONGO_PORT_CHRONOS", "0"))
MONGO_USERNAME = os.getenv("MONGO_USER_CHRONOS", "")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD_CHRONOS", "")
MONGO_DATABASE = os.getenv("MONGO_DATABASE_CHRONOS", "")


class _MongoDB:
    def __init__(self) -> None:
        self._client: Optional[MongoClient] = None

    @property
    def client(self) -> Optional[MongoClient]:
        """client property"""
        if self._client is None:
            raise AttributeError("First you need to `mongodb.init_client()`")
        return self._client

    @property
    def activity_sessions_collection(self) -> Collection:
        """activity_sessions_collection property"""
        if self._client is None:
            raise AttributeError("First you need to `mongodb.init_client()`")
        return self._client[MONGO_DATABASE].activity_sessions

    def init_client(self) -> None:
        """Used for initializing MongoDB Client based on .env configuration."""
        if not self._client:
            self._client = MongoClient(
                host=MONGO_HOST,
                port=MONGO_PORT,
                username=MONGO_USERNAME,
                password=MONGO_PASSWORD,
            )


mongodb = _MongoDB()
