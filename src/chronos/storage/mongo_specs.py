from dataclasses import dataclass
import json
from typing import Dict, Any, List
from datetime import datetime

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
import pymongo.errors

from chronos import settings


class _MaterializedView(Collection):  # type: ignore[misc]
    def __init__(
        self,
        name: str,
        match_stage_conds: Dict[str, Any],
        database_: Database,
        **kwargs: Any,
    ) -> None:

        super().__init__(database=database_, name=name, **kwargs)

        self.match_stage_conds = match_stage_conds

    def run_aggregation(self, collection: Collection, reference_time: datetime) -> None:
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


@dataclass
class _CollectionsBase:
    def __post_init__(self) -> None:
        self.names = self.__annotations__.keys()  # pylint: disable=no-member

    def to_list(self) -> List[Collection]:
        return [self.__getattribute__(name) for name in self.names]


@dataclass
class _Collections(_CollectionsBase):
    activity_sessions: Collection
    user_generation_failed: Collection
    generations: Collection


@dataclass
class _MaterializedViews(_CollectionsBase):
    learning_time_sessions_duration_mv: _MaterializedView
    break_sessions_duration_mv: _MaterializedView
    focus_sessions_duration_mv: _MaterializedView


def _initialize_collections(database_: Database) -> _Collections:

    collections_names = list(
        _Collections.__annotations__.keys()  # pylint: disable=no-member
    )

    for collection_name in collections_names:
        if collection_name not in database_.list_collection_names():
            validator_file_path = (
                settings.CHRONOS_PACKAGE_DIR
                / f"storage/schema_validators/{collection_name}.json"
            )

            with open(validator_file_path, "r") as file:
                validator = json.loads(file.read())

            database_.create_collection(collection_name, validator=validator)

    return _Collections(
        *(database_.get_collection(name=col_name) for col_name in collections_names)
    )


def _initialize_materialized_views(database_: Database) -> _MaterializedViews:

    materialized_views_confs: Dict[str, Any] = {
        "learning_time_sessions_duration_mv": {
            "$or": [{"is_active": {"$eq": True}}, {"is_break": {"$eq": True}}]
        },
        "break_sessions_duration_mv": {"is_break": {"$eq": True}},
        "focus_sessions_duration_mv": {"is_focus": {"$eq": True}},
    }

    return _MaterializedViews(
        **{
            name: _MaterializedView(
                name=name,
                match_stage_conds=conf,
                database_=database_,
            )
            for name, conf in materialized_views_confs.items()
        }
    )


client = MongoClient(
    host=settings.MONGO_HOST,
    port=settings.MONGO_PORT,
    username=settings.MONGO_USERNAME,
    password=settings.MONGO_PASSWORD,
)
try:
    client.admin.command("replSetGetStatus")
except pymongo.errors.OperationFailure as err:
    if err.details["codeName"] == "NotYetInitialized":
        client.admin.command("replSetInitiate")

database = client[settings.MONGO_DATABASE]

collections = _initialize_collections(database_=database)
materialized_views = _initialize_materialized_views(database_=database)
