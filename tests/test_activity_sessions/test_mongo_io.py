"""Whole activity_sessions creation flow for one user."""
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=duplicate-code

from datetime import datetime
from typing import Dict, List, Union
import unittest.mock

import bson  # type: ignore[import]
import pandas as pd  # type: ignore[import]
import pymongo.cursor  # type: ignore[import]
import pytest
from pythink_toolbox.testing.parametrization import parametrize, Scenario

from chronos.activity_sessions.create_activity_sessions import ActivitySession
import chronos.activity_sessions.mongo_io as tested_module


TEST_USER_ID = 1


tested_module.ACTIVITY_SESSIONS_COLLECTION.delete_many({})


class MainScenario(Scenario):
    activity_events: pd.Series
    expected_data: List[ActivitySession]


TEST_DATA = [
    MainScenario(
        desc="Initial input - creates two separate active sessions and inactive in the middle",
        activity_events=pd.Series([datetime(2000, 1, 1), datetime(2000, 1, 2)]),
        expected_data=[
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(1999, 12, 31, 23, 59),
                "end_time": datetime(2000, 1, 1, 0, 0),
                "is_active": True,
                "is_focus": False,
                "is_break": False,
            },
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(2000, 1, 1, 0, 0),
                "end_time": datetime(2000, 1, 1, 23, 59),
                "is_active": False,
                "is_focus": False,
                "is_break": False,
            },
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(2000, 1, 1, 23, 59),
                "end_time": datetime(2000, 1, 2, 0, 0),
                "is_active": True,
                "is_focus": False,
                "is_break": False,
            },
        ],
    ),
    MainScenario(
        desc="Takes last active session & extends its duration.",
        activity_events=pd.Series([datetime(2000, 1, 2, 0, 5)]),
        expected_data=[
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(1999, 12, 31, 23, 59),
                "end_time": datetime(2000, 1, 1, 0, 0),
                "is_active": True,
                "is_focus": False,
                "is_break": False,
            },
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(2000, 1, 1, 0, 0),
                "end_time": datetime(2000, 1, 1, 23, 59),
                "is_active": False,
                "is_focus": False,
                "is_break": False,
            },
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(2000, 1, 1, 23, 59),
                "end_time": datetime(2000, 1, 2, 0, 5),
                "is_active": True,
                "is_focus": False,
                "is_break": False,
            },
        ],
    ),
    MainScenario(
        desc="Takes last active session & extends its duration, so it changes to focus session.",
        activity_events=pd.Series(
            [datetime(2000, 1, 2, 0, 10), datetime(2000, 1, 2, 0, 15)]
        ),
        expected_data=[
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(1999, 12, 31, 23, 59),
                "end_time": datetime(2000, 1, 1, 0, 0),
                "is_active": True,
                "is_focus": False,
                "is_break": False,
            },
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(2000, 1, 1, 0, 0),
                "end_time": datetime(2000, 1, 1, 23, 59),
                "is_active": False,
                "is_focus": False,
                "is_break": False,
            },
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(2000, 1, 1, 23, 59),
                "end_time": datetime(2000, 1, 2, 0, 15),
                "is_active": True,
                "is_focus": True,
                "is_break": False,
            },
        ],
    ),
    MainScenario(
        desc="Add new sessions one focused in the end and on that is 'break' before.",
        activity_events=pd.Series(
            [
                datetime(2000, 1, 2, 0, 21, 1),
                datetime(2000, 1, 2, 0, 25),
                datetime(2000, 1, 2, 0, 30),
                datetime(2000, 1, 2, 0, 35),
                datetime(2000, 1, 2, 0, 40),
            ]
        ),
        expected_data=[
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(1999, 12, 31, 23, 59),
                "end_time": datetime(2000, 1, 1, 0, 0),
                "is_active": True,
                "is_focus": False,
                "is_break": False,
            },
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(2000, 1, 1, 0, 0),
                "end_time": datetime(2000, 1, 1, 23, 59),
                "is_active": False,
                "is_focus": False,
                "is_break": False,
            },
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(2000, 1, 1, 23, 59),
                "end_time": datetime(2000, 1, 2, 0, 15),
                "is_active": True,
                "is_focus": True,
                "is_break": False,
            },
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(2000, 1, 2, 0, 15),
                "end_time": datetime(2000, 1, 2, 0, 20, 1),
                "is_active": False,
                "is_focus": False,
                "is_break": True,
            },
            {
                "user_id": TEST_USER_ID,
                "start_time": datetime(2000, 1, 2, 0, 20, 1),
                "end_time": datetime(2000, 1, 2, 0, 40),
                "is_active": True,
                "is_focus": True,
                "is_break": False,
            },
        ],
    ),
]

# FIXME improve ci
@pytest.mark.integration
@parametrize(TEST_DATA)  # type: ignore[misc]
def test_main(activity_events: pd.Series, expected_data: List[ActivitySession]) -> None:
    tested_module.main(
        user_id=TEST_USER_ID,
        activity_events=activity_events,
        start_time=unittest.mock.ANY,
    )

    activity_sessions_collection_content = (
        tested_module.ACTIVITY_SESSIONS_COLLECTION.find()
    )
    data = _filter_id_field(query_result=activity_sessions_collection_content)

    assert data == expected_data


def _filter_id_field(
    query_result: pymongo.cursor.Cursor,
) -> List[Dict[str, Union[int, datetime, bool, bson.ObjectId]]]:
    return [
        {key: value for key, value in document.items() if key != "_id"}
        for document in query_result
    ]
