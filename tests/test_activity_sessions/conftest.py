from typing import List, Dict, Union, Callable
from datetime import datetime

from pymongo.cursor import Cursor
import pytest

from chronos.storage.specs import mongodb
from chronos.storage.schemas import (
    ActivitySessionSchema,
    MaterializedViewSchema,
    UserGenerationFailedSchema,
)
from chronos import settings


def _filter_id_field(
    query_result: Cursor,
) -> List[Dict[str, Union[int, datetime, bool]]]:
    return [
        {key: value for key, value in document.items() if key != "_id"}
        for document in query_result
    ]


def _get_activity_session_collection_content_without_id() -> List[
    Dict[str, Union[int, datetime, bool]]
]:

    activity_sessions_collection_content = mongodb.collections.activity_sessions.find()

    return _filter_id_field(query_result=activity_sessions_collection_content)


@pytest.fixture
def get_activity_session_collection_content_without_id() -> Callable[
    [], List[Dict[str, Union[int, datetime, bool]]]
]:
    """Returns function that query Mongo activity_sessions collection
    and return all its content"""

    return _get_activity_session_collection_content_without_id


def _clear_storage() -> None:
    mongodb.client.drop_database(settings.MONGO_DATABASE)


@pytest.fixture
def clear_storage() -> Callable[[], None]:
    """Clears whole activity_sessions collection."""
    return _clear_storage


def _get_materialized_view_content(
    materialized_view_name: str,
) -> List[MaterializedViewSchema]:

    materialized_view = mongodb.database[materialized_view_name]
    return list(materialized_view.find())


@pytest.fixture
def get_materialized_view_content() -> Callable[[str], List[MaterializedViewSchema]]:
    """Returns function that query materialized view and return all its content"""

    return _get_materialized_view_content


def _insert_data_to_activity_sessions_collection(
    data: List[ActivitySessionSchema],
) -> None:

    mongodb.collections.activity_sessions.insert_many(data)


@pytest.fixture
def insert_data_to_activity_sessions_collection() -> Callable[
    [List[ActivitySessionSchema]], None
]:
    """Inserts data to activity_sessions collection."""
    return _insert_data_to_activity_sessions_collection


def _read_failed_generation_collection_content() -> List[UserGenerationFailedSchema]:

    return list(mongodb.collections.user_generation_failed.find({}))


@pytest.fixture
def read_failed_generation_collection_content() -> Callable[
    [], List[UserGenerationFailedSchema]
]:
    """Returns content of user_generation_failed collection."""
    return _read_failed_generation_collection_content
