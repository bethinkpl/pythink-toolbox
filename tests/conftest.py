from typing import List, Dict, Union, Callable, Iterator
from datetime import datetime

from pymongo.cursor import Cursor
import pytest

from chronos.storage.specs import mongodb
from chronos.storage.schemas import (
    ActivitySessionSchema,
    MaterializedViewSchema,
)
from chronos import settings


def _filter_id_field(
    query_result: Cursor,
) -> List[Dict[str, Union[int, datetime, bool]]]:
    return [
        {key: value for key, value in document.items() if key != "_id"}
        for document in query_result
    ]


@pytest.fixture
def get_activity_session_collection_content_without_id() -> Callable[
    [], List[Dict[str, Union[int, datetime, bool]]]
]:
    """Returns function that query Mongo activity_sessions collection
    and return all its content"""

    def _get_activity_session_collection_content_without_id() -> List[
        Dict[str, Union[int, datetime, bool]]
    ]:
        activity_sessions_collection_content = (
            mongodb.collections.activity_sessions.find()
        )

        return _filter_id_field(query_result=activity_sessions_collection_content)

    return _get_activity_session_collection_content_without_id


@pytest.fixture(name="clear_storage_factory")
def clear_storage_factory_as_fixture() -> Callable[[], None]:
    """Clears whole db.
    https://docs.pytest.org/en/stable/fixture.html#factories-as-fixtures"""

    def _clear_storage() -> None:
        mongodb.client.drop_database(settings.MONGO_DATABASE)

    return _clear_storage


@pytest.fixture
def clear_storage(clear_storage_factory: Callable[[], None]) -> Iterator[None]:
    """Clears whole db before and test call."""
    clear_storage_factory()
    yield
    clear_storage_factory()


@pytest.fixture
def get_materialized_view_content() -> Callable[[str], List[MaterializedViewSchema]]:
    """Returns function that query materialized view and return all its content"""

    def _get_materialized_view_content(
        materialized_view_name: str,
    ) -> List[MaterializedViewSchema]:
        materialized_view = mongodb.database[materialized_view_name]
        return list(materialized_view.find())

    return _get_materialized_view_content


@pytest.fixture
def insert_data_to_activity_sessions_collection() -> Callable[
    [List[ActivitySessionSchema]], None
]:
    """Inserts data to activity_sessions collection."""

    def _insert_data_to_activity_sessions_collection(
        data: List[ActivitySessionSchema],
    ) -> None:
        mongodb.collections.activity_sessions.insert_many(data)

    return _insert_data_to_activity_sessions_collection
