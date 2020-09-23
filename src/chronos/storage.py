from dataclasses import dataclass
from typing import Optional

import datatosk.types
from pymongo import MongoClient
import pymongo.collection

from chronos.settings import (
    MONGO_DATABASE,
    MONGO_HOST,
    MONGO_PORT,
    MONGO_USERNAME,
    MONGO_PASSWORD,
)


@dataclass
class MaterializedView:
    name: str
    match_stage_conds: datatosk.types.JSONType  # FIXME extract types from datatosk


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


def get_activity_sessions_collection() -> pymongo.collection.Collection:
    """Returns activity_sessions collection"""
    return get_client()[MONGO_DATABASE].activity_sessions


materialized_views = (
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

get_client = _MongoDBClient()
