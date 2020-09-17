from typing import Optional

from pymongo import MongoClient
from pymongo.collection import Collection

from chronos.settings import (
    MONGO_DATABASE,
    MONGO_HOST,
    MONGO_PORT,
    MONGO_USERNAME,
    MONGO_PASSWORD,
)


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
