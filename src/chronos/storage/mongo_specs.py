from dataclasses import dataclass
import json
import time
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
    users_generation_statuses: Collection
    generations: Collection


@dataclass
class _MaterializedViews(_CollectionsBase):
    learning_time_sessions_duration_mv: _MaterializedView
    break_sessions_duration_mv: _MaterializedView
    focus_sessions_duration_mv: _MaterializedView


class _MongoSpecs:
    def __init__(self):
        self._client = None
        self._database = None
        self._collections = None
        self._materialized_views = None

    @property
    def client(self) -> MongoClient:
        if self._client is None:
            self._init_client()
        return self._client

    @property
    def database(self) -> Database:
        if self._database is None:
            self._database = self.client[settings.MONGO_DATABASE]
        return self._database

    @property
    def collections(self) -> _Collections:
        if self._collections is None:
            self._init_collections()
        return self._collections

    @property
    def materialized_views(self) -> _MaterializedViews:
        if self._materialized_views is None:
            self._init_materialized_views()
        return self._materialized_views

    def _init_client(self):

        self._client = MongoClient(
            host=settings.MONGO_HOST,
            port=settings.MONGO_PORT,
            username=settings.MONGO_USERNAME,
            password=settings.MONGO_PASSWORD,
        )
        try:
            self._client.admin.command("replSetGetStatus")
        except pymongo.errors.OperationFailure as err:
            if err.details["codeName"] == "NotYetInitialized":
                self._client.admin.command("replSetInitiate")

        time.sleep(1)

    def _init_collections(self) -> None:

        collections_names = list(
            _Collections.__annotations__.keys()  # pylint: disable=no-member
        )

        for collection_name in collections_names:
            if collection_name not in self.database.list_collection_names():
                validator_file_path = (
                    settings.CHRONOS_PACKAGE_DIR
                    / f"storage/schema_validators/{collection_name}.json"
                )

                with open(validator_file_path, "r") as file:
                    validator = json.loads(file.read())

                self.database.create_collection(collection_name, validator=validator)

        self._collections = _Collections(
            *(
                self.database.get_collection(name=col_name)
                for col_name in collections_names
            )
        )

    def _init_materialized_views(self):

        materialized_views_confs: Dict[str, Any] = {
            "learning_time_sessions_duration_mv": {
                "$or": [{"is_active": {"$eq": True}}, {"is_break": {"$eq": True}}]
            },
            "break_sessions_duration_mv": {"is_break": {"$eq": True}},
            "focus_sessions_duration_mv": {"is_focus": {"$eq": True}},
        }

        self._materialized_views = _MaterializedViews(
            **{
                name: _MaterializedView(
                    name=name,
                    match_stage_conds=conf,
                    database_=self.database,
                )
                for name, conf in materialized_views_confs.items()
            }
        )


mongo_specs = _MongoSpecs()
