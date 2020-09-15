"""Whole activity_sessions creation flow for one user."""
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=duplicate-code

from datetime import datetime
from typing import Dict, List, Union, Callable
import unittest.mock

import pandas as pd
import pytest
from pythink_toolbox.testing.parametrization import parametrize, Scenario

from chronos.activity_sessions.create_activity_sessions import ActivitySession
import chronos.activity_sessions.mongo_io as tested_module


TEST_USER_ID = 108


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


@pytest.mark.integration  # type: ignore[misc]
@parametrize(TEST_DATA)  # type: ignore[misc]
def test_main(
    activity_events: pd.Series,
    expected_data: List[ActivitySession],
    get_activity_session_collection_content_without_id: Callable[
        [], List[Dict[str, Union[int, datetime, bool]]]
    ],
) -> None:

    tested_module.main(
        user_id=TEST_USER_ID,
        activity_events=activity_events,
        start_time=unittest.mock.ANY,
    )

    data = get_activity_session_collection_content_without_id()

    assert data == expected_data
