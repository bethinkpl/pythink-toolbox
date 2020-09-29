from dataclasses import dataclass
from typing import Optional, Sequence, Any, Dict, TypedDict
from datetime import datetime

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from chronos import settings

ACTIVITY_SESSIONS_COLLECTION_NAME = "activity_sessions"


class StorageError(Exception):
    """Error related to storage."""


class MongoCommitError(StorageError):
    """Error that occurred while performing MongoDB commit."""


class _MongoDB:
    def __init__(self) -> None:
        self._client: Optional[MongoClient] = None

    @property
    def client(self) -> MongoClient:
        """
        Returns:
            pymongo.MongoClient
        """
        if self._client is None:
            self._client = MongoClient(
                host=settings.MONGO_HOST,
                port=settings.MONGO_PORT,
                username=settings.MONGO_USERNAME,
                password=settings.MONGO_PASSWORD,
            )

        return self._client

    @property
    def database(self) -> Database:
        """
        Returns:
            pymongo.database.Database
        """
        if self._client is None:
            raise StorageError("Client is not initialized.")
        return self._client[settings.MONGO_DATABASE]

    @property
    def activity_sessions_collection(self) -> Collection:
        """
        Returns:
            pymongo.collection.Collection
        """
        return self.database.get_collection(ACTIVITY_SESSIONS_COLLECTION_NAME)


mongodb = _MongoDB()


@dataclass
class MaterializedView:
    name: str
    match_stage_conds: Dict[str, Any]

    def __post_init__(self) -> None:
        self.name = self.name + "_mv"

    def update(self, collection: Collection, reference_time: datetime) -> None:
        """Updates materialized view content based on `reference time`."""
        match_stage = {
            **self.match_stage_conds,
            "end_time": {"$gte": reference_time},
        }

        project_stage = {
            "_id": {"user_id": "$user_id", "start_time": "$start_time"},
            "end_time": 1,
            "duration_ms": {"$sum": {"$subtract": ["$end_time", "$start_time"]}},
        }

        merge_stage = {"into": self.name, "whenMatched": "replace"}

        collection.aggregate(
            [
                {"$match": match_stage},
                {"$project": project_stage},
                {"$merge": merge_stage},
            ]
        )


class MaterializedViewIDSchema(TypedDict):
    user_id: int
    start_time: datetime


class MaterializedViewSchema(TypedDict):
    _id: MaterializedViewIDSchema
    end_time: datetime
    duration_ms: int


materialized_views: Sequence[MaterializedView] = (
    MaterializedView(
        name="learning_time_sessions_duration",
        match_stage_conds={
            "$or": [{"is_active": {"$eq": True}}, {"is_break": {"$eq": True}}]
        },
    ),
    MaterializedView(
        name="break_sessions_duration",
        match_stage_conds={"is_break": {"$eq": True}},
    ),
    MaterializedView(
        name="focus_sessions_duration",
        match_stage_conds={"is_focus": {"$eq": True}},
    ),
)
