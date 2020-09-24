from dataclasses import dataclass
from typing import Optional, Sequence, Any, Dict, Callable
from datetime import datetime

from pymongo import MongoClient
from pymongo.collection import Collection

from chronos.settings import (
    MONGO_DATABASE,
    MONGO_HOST,
    MONGO_PORT,
    MONGO_USERNAME,
    MONGO_PASSWORD,
)


class _MongoDBClient:  # pylint: disable=too-few-public-methods
    def __init__(self) -> None:
        self._client: Optional[MongoClient] = None

    def __call__(self) -> MongoClient:
        if not self._client:
            self._client = MongoClient(
                host=MONGO_HOST,
                port=MONGO_PORT,
                username=MONGO_USERNAME,
                password=MONGO_PASSWORD,
            )

        return self._client


@dataclass
class MaterializedView:
    name: str
    match_stage_conds: Dict[str, Any]

    def update(self, collection: Collection, reference_time: datetime):
        match_stage = {
            **self.match_stage_conds,
            "end_time": {"$gte": reference_time},
        }

        project_stage = {
            "_id": {
                "user_id": "$user_id",
                "start_time": "$start_time",
                "end_time": "$end_time",
            },
            "duration_ms": {"$sum": {"$subtract": ["$end_time", "$start_time"]}},
        }

        merge_stage = {"into": self.name + "_mv", "whenMatched": "replace"}

        collection.aggregate(
            [
                {"$match": match_stage},
                {"$project": project_stage},
                {"$merge": merge_stage},
            ]
        )


def get_activity_sessions_collection() -> Collection:
    """Returns activity_sessions collection"""
    return get_client()[MONGO_DATABASE].activity_sessions


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

get_client: Callable[[], MongoClient] = _MongoDBClient()
