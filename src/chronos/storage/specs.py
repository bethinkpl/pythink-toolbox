from dataclasses import dataclass
import json
from typing import Optional, Sequence, Any, Dict
from datetime import datetime

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from chronos import settings


@dataclass
class _MaterializedView:
    name: str
    match_stage_conds: Dict[str, Any]

    def __post_init__(self) -> None:
        self.name = self.name + "_mv"

    def update(self, reference_time: datetime) -> None:
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

        mongodb.collections.activity_sessions.aggregate(
            [
                {"$match": match_stage},
                {"$project": project_stage},
                {"$merge": merge_stage},
            ]
        )


class _MongoDB:
    @dataclass
    class Collections:
        activity_sessions: Collection
        user_generation_failed: Collection

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
            raise AttributeError("Client is not initialized.")
        return self._client[settings.MONGO_DATABASE]

    @property
    def collections(self) -> Collections:
        """
        Returns:
            NamedTuple of collections.
        """
        activity_sessions_name = "activity_sessions"

        if activity_sessions_name not in self.database.list_collection_names():
            self._create_validated_collection(activity_sessions_name)

        return self.Collections(
            activity_sessions=self.database.get_collection("activity_sessions"),
            user_generation_failed=self.database.user_generation_failed,  # TODO add user_generation_failed schema validation
        )

    @property
    def materialized_views(self) -> Sequence[_MaterializedView]:
        """
        Returns:
            Tuple of all MongoDB materialized views.
        """
        return (
            _MaterializedView(
                name="learning_time_sessions_duration",
                match_stage_conds={
                    "$or": [{"is_active": {"$eq": True}}, {"is_break": {"$eq": True}}]
                },
            ),
            _MaterializedView(
                name="break_sessions_duration",
                match_stage_conds={"is_break": {"$eq": True}},
            ),
            _MaterializedView(
                name="focus_sessions_duration",
                match_stage_conds={"is_focus": {"$eq": True}},
            ),
        )

    def _create_validated_collection(self, name: str) -> None:

        validator_file_path = (
            settings.CHRONOS_PACKAGE_DIR / f"storage/schema_validators/{name}.json"
        )

        with open(validator_file_path, "r") as file:
            validator = json.loads(file.read())

        self.database.create_collection(name, validator=validator)


mongodb = _MongoDB()
