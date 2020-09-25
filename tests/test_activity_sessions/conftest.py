from typing import List, Dict, Union, Callable
from datetime import datetime

from pymongo.cursor import Cursor
import pytest

from chronos import storage


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

    activity_sessions_collection_content = (
        storage.get_activity_sessions_collection().find()
    )

    return _filter_id_field(query_result=activity_sessions_collection_content)


@pytest.fixture  # type: ignore[misc]
def get_activity_session_collection_content_without_id() -> Callable[
    [], List[Dict[str, Union[int, datetime, bool]]]
]:
    """Returns function that query Mongo activity_sessions collection
    and return all its content"""

    return _get_activity_session_collection_content_without_id


def _clear_storage() -> None:
    storage.get_client().drop_database(storage.MONGO_DATABASE)


@pytest.fixture  # type: ignore[misc]
def clear_storage() -> Callable[[], None]:
    """Clears whole activity_sessions collection."""
    return _clear_storage


def _get_materialized_view_content(
    materialized_view_name: str,
) -> List[storage.MaterializedViewSchema]:

    materialized_view = storage.get_chronos_db()[materialized_view_name]
    return list(materialized_view.find())


@pytest.fixture
def get_materialized_view_content() -> Callable[
    [str], List[storage.MaterializedViewSchema]
]:
    """Returns function that query materialized view and return all its content"""

    return _get_materialized_view_content
