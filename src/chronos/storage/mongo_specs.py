from dataclasses import dataclass
import json
from time import sleep
from typing import Dict, Any, List, Optional
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
    """Main storage class, containing all Mongo DB's specifications."""

    def __init__(self) -> None:
        self._client: Optional[MongoClient] = None
        self._database: Optional[Database] = None
        self._collections: Optional[_Collections] = None
        self._materialized_views: Optional[_MaterializedViews] = None

    @property
    def client(self) -> MongoClient:
        """
        Returns:
            MongoClient object specified in configuration.
        """

        if self._client is None:
            self._client = self._init_client()
        return self._client

    @property
    def database(self) -> Database:
        """
        Returns:
            Mongo database specified in configuration.
        """

        if self._database is None:
            self._database = self.client[settings.MONGO_DATABASE]
        return self._database

    @property
    def collections(self) -> _Collections:
        """
        Returns:
            Mongo collections as attributes of this object
            specified in _Collections class.
        Examples:
            To get `activity_sessions` object:
            >>> _MongoSpecs().collections.activity_sessions
        """

        if self._collections is None:
            self._collections = self._init_collections()
        return self._collections

    @property
    def materialized_views(self) -> _MaterializedViews:
        """
        Returns:
            Mongo materialized_views as attributes of this object
            specified in _Collections class.
        Examples:
            To get `activity_sessions` object:
            >>> _MongoSpecs().materialized_views.activity_sessions
        """

        if self._materialized_views is None:
            self._materialized_views = self._init_materialized_views()
        return self._materialized_views

    @staticmethod
    def _init_client() -> MongoClient:

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

        # mongo needs some time to figure out replication
        # https://pymongo.readthedocs.io/en/stable/examples/high_availability.html#id1
        sleep(1)

        return client

    def _init_collections(self) -> _Collections:

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

        return _Collections(
            *(
                self.database.get_collection(name=col_name)
                for col_name in collections_names
            )
        )

    def _init_materialized_views(self) -> _MaterializedViews:

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
                    database_=self.database,
                )
                for name, conf in materialized_views_confs.items()
            }
        )


mongo_specs = _MongoSpecs()
