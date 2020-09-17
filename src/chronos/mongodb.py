from typing import Optional

from pymongo import MongoClient

from chronos.settings import (
    MONGO_DATABASE,
    MONGO_HOST,
    MONGO_PORT,
    MONGO_USERNAME,
    MONGO_PASSWORD,
)


class _MongoDBClient:
    def __init__(self) -> None:
        self._client: Optional[MongoClient] = None

    def __call__(self):
        if not self._client:
            self._client = MongoClient(
                host=MONGO_HOST,
                port=MONGO_PORT,
                username=MONGO_USERNAME,
                password=MONGO_PASSWORD,
            )

        return self._client


def get_activity_sessions_collection():
    return get_client()[MONGO_DATABASE].activity_sessions


get_client = _MongoDBClient()
