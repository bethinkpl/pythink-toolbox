from typing import List, Dict, Union, Callable
from datetime import datetime

from pymongo.cursor import Cursor
import pytest

import chronos.settings


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
        chronos.settings.ACTIVITY_SESSIONS_COLLECTION.find()
    )

    return _filter_id_field(query_result=activity_sessions_collection_content)


@pytest.fixture  # type: ignore[misc]
def get_activity_session_collection_content_without_id() -> Callable[
    [], List[Dict[str, Union[int, datetime, bool]]]
]:
    """Fixture that returns function that query Mongo activity_sessions collection
    and return all its content"""

    return _get_activity_session_collection_content_without_id


chronos.settings.ACTIVITY_SESSIONS_COLLECTION.delete_many({})
