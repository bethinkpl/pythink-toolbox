# pylint: disable=import-outside-toplevel
import os
from typing import List, Dict, Union, Callable, Iterator, Any
from datetime import datetime

from pymongo.cursor import Cursor
import pytest

from chronos.storage import schemas


os.environ["CHRONOS_MONGO_DATABASE"] = "test_db"


def _filter_id_field(
    query_result: Cursor,
) -> List[Dict[str, Union[int, datetime, bool]]]:
    return [
        {key: value for key, value in document.items() if key != "_id"}
        for document in query_result
    ]


@pytest.fixture(name="get_collection_content_without_id_factory")
def get_collection_content_without_id_factory_as_fixture() -> Callable[
    [str], List[Dict[str, Any]]
]:
    """Returns function that query Mongo activity_sessions collection
    and return all its content"""
    from chronos.storage.mongo_specs import collections

    def _get_collection_content_without_id(
        collection_name: str,
    ) -> List[Dict[str, Union[int, datetime, bool]]]:
        return _filter_id_field(query_result=collections[collection_name].find())

    return _get_collection_content_without_id


@pytest.fixture(name="clear_storage_factory")
def clear_storage_factory_as_fixture() -> Callable[[], None]:
    """Clears whole db.
    https://docs.pytest.org/en/stable/fixture.html#factories-as-fixtures"""
    from chronos.storage.mongo_specs import collections, materialized_views

    def _clear_storage() -> None:
        for collection in {**collections, **materialized_views}.values():
            collection.delete_many({})

    return _clear_storage


@pytest.fixture
def clear_storage(clear_storage_factory: Callable[[], None]) -> Iterator[None]:
    """Clears whole db before and test call."""
    clear_storage_factory()
    yield
    clear_storage_factory()


@pytest.fixture(name="get_materialized_view_content_factory")
def get_materialized_view_content_factory_as_fixture() -> Callable[
    [str], List[schemas.MaterializedViewSchema]
]:
    """Returns function that query materialized view and return all its content."""
    from chronos.storage.mongo_specs import database

    def _get_materialized_view_content(
        materialized_view_name: str,
    ) -> List[schemas.MaterializedViewSchema]:
        materialized_view = database[materialized_view_name]
        return list(materialized_view.find())

    return _get_materialized_view_content


@pytest.fixture(name="insert_data_to_activity_sessions_collection_factory")
def insert_data_to_activity_sessions_collection_factory_as_fixture() -> Callable[
    [List[schemas.ActivitySessionSchema]], None
]:
    """Inserts data to activity_sessions collection."""
    from chronos.storage.mongo_specs import collections

    def _insert_data_to_activity_sessions_collection(
        data: List[schemas.ActivitySessionSchema],
    ) -> None:
        collections["activity_sessions"].insert_many(data)

    return _insert_data_to_activity_sessions_collection
