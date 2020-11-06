from collections import namedtuple
from dataclasses import dataclass
import json
from typing import Dict, Sequence, Any
from datetime import datetime

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from chronos import settings


_MaterializedViewConf = namedtuple(
    "MaterializedViewConf", ["name", "match_stage_conds"]
)


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


def _initialize_collections(database_: Database) -> "_Collections":
    @dataclass
    class _Collections:
        activity_sessions: Collection
        user_generation_failed: Collection
        generations: Collection

    collections_names = list(_Collections.__annotations__.keys())

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
        *(database_.get_collection(collection) for collection in collections_names)
    )


def _initialize_materialized_views(
    materialized_views_confs: Sequence[_MaterializedViewConf], database_: Database
) -> Dict[str, _MaterializedView]:

    return {
        materialized_view_conf.name: _MaterializedView(
            name=materialized_view_conf.name,
            match_stage_conds=materialized_view_conf.match_stage_conds,
            database_=database_,
        )
        for materialized_view_conf in materialized_views_confs
    }


client = MongoClient(
    host=settings.MONGO_HOST,
    port=settings.MONGO_PORT,
    username=settings.MONGO_USERNAME,
    password=settings.MONGO_PASSWORD,
)

database = client[settings.MONGO_DATABASE]

collections = _initialize_collections(database_=database)

materialized_views = _initialize_materialized_views(
    materialized_views_confs=(
        _MaterializedViewConf(
            name="learning_time_sessions_duration_mv",
            match_stage_conds={
                "$or": [{"is_active": {"$eq": True}}, {"is_break": {"$eq": True}}]
            },
        ),
        _MaterializedViewConf(
            name="break_sessions_duration_mv",
            match_stage_conds={"is_break": {"$eq": True}},
        ),
        _MaterializedViewConf(
            name="focus_sessions_duration_mv",
            match_stage_conds={"is_focus": {"$eq": True}},
        ),
    ),
    database_=database,
)
