"""Whole activity_sessions creation flow for one user."""
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=duplicate-code

from datetime import datetime
from typing import Dict, List, Union, Callable

import pandas as pd
import pytest
from pythink_toolbox.testing.parametrization import Scenario

from chronos.activity_sessions.creation_transformations import ActivitySession
import chronos.activity_sessions.storage_operations as tested_module

from chronos.storage import MaterializedViewSchema

TEST_USER_ID = 108


class MainScenario(Scenario):
    activity_events: pd.Series
    expected_collection_data: List[ActivitySession]
    expected_learning_time_sessions_duration_mv_data: List[MaterializedViewSchema]


TEST_STEPS = [
    MainScenario(
        desc="Initial input - creates two separate active sessions and inactive in the middle",
        activity_events=pd.Series([datetime(2000, 1, 1), datetime(2000, 1, 2)]),
        expected_collection_data=[
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
        expected_learning_time_sessions_duration_mv_data=[
            {
                "_id": {
                    "user_id": TEST_USER_ID,
                    "start_time": datetime(1999, 12, 31, 23, 59),
                },
                "end_time": datetime(2000, 1, 1, 0, 0),
                "duration_ms": (
                    datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
                ).total_seconds()
                * 1000,
            },
            {
                "_id": {
                    "user_id": TEST_USER_ID,
                    "start_time": datetime(2000, 1, 1, 23, 59),
                },
                "end_time": datetime(2000, 1, 2, 0, 0),
                "duration_ms": (
                    datetime(2000, 1, 2, 0, 0) - datetime(2000, 1, 1, 23, 59)
                ).total_seconds()
                * 1000,
            },
        ],
    ),
    MainScenario(
        desc="Takes last active session & extends its duration.",
        activity_events=pd.Series([datetime(2000, 1, 2, 0, 5)]),
        expected_collection_data=[
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
        expected_learning_time_sessions_duration_mv_data=[
            {
                "_id": {
                    "user_id": TEST_USER_ID,
                    "start_time": datetime(1999, 12, 31, 23, 59),
                },
                "end_time": datetime(2000, 1, 1, 0, 0),
                "duration_ms": (
                    datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
                ).total_seconds()
                * 1000,
            },
            {
                "_id": {
                    "user_id": TEST_USER_ID,
                    "start_time": datetime(2000, 1, 1, 23, 59),
                },
                "end_time": datetime(2000, 1, 2, 0, 5),
                "duration_ms": (
                    datetime(2000, 1, 2, 0, 5) - datetime(2000, 1, 1, 23, 59)
                ).total_seconds()
                * 1000,
            },
        ],
    ),
    MainScenario(
        desc="Takes last active session & extends its duration, so it changes to focus session.",
        activity_events=pd.Series(
            [datetime(2000, 1, 2, 0, 10), datetime(2000, 1, 2, 0, 15)]
        ),
        expected_collection_data=[
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
        expected_learning_time_sessions_duration_mv_data=[
            {
                "_id": {
                    "user_id": TEST_USER_ID,
                    "start_time": datetime(1999, 12, 31, 23, 59),
                },
                "end_time": datetime(2000, 1, 1, 0, 0),
                "duration_ms": (
                    datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
                ).total_seconds()
                * 1000,
            },
            {
                "_id": {
                    "user_id": TEST_USER_ID,
                    "start_time": datetime(2000, 1, 1, 23, 59),
                },
                "end_time": datetime(2000, 1, 2, 0, 15),
                "duration_ms": (
                    datetime(2000, 1, 2, 0, 15) - datetime(2000, 1, 1, 23, 59)
                ).total_seconds()
                * 1000,
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
        expected_collection_data=[
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
        expected_learning_time_sessions_duration_mv_data=[
            {
                "_id": {
                    "user_id": TEST_USER_ID,
                    "start_time": datetime(1999, 12, 31, 23, 59),
                },
                "end_time": datetime(2000, 1, 1, 0, 0),
                "duration_ms": (
                    datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
                ).total_seconds()
                * 1000,
            },
            {
                "_id": {
                    "user_id": TEST_USER_ID,
                    "start_time": datetime(2000, 1, 1, 23, 59),
                },
                "end_time": datetime(2000, 1, 2, 0, 15),
                "duration_ms": (
                    datetime(2000, 1, 2, 0, 15) - datetime(2000, 1, 1, 23, 59)
                ).total_seconds()
                * 1000,
            },
            {
                "_id": {
                    "user_id": TEST_USER_ID,
                    "start_time": datetime(2000, 1, 2, 0, 15),
                },
                "end_time": datetime(2000, 1, 2, 0, 20, 1),
                "duration_ms": (
                    datetime(2000, 1, 2, 0, 20, 1) - datetime(2000, 1, 2, 0, 15)
                ).total_seconds()
                * 1000,
            },
            {
                "_id": {
                    "user_id": TEST_USER_ID,
                    "start_time": datetime(2000, 1, 2, 0, 20, 1),
                },
                "end_time": datetime(2000, 1, 2, 0, 40),
                "duration_ms": (
                    datetime(2000, 1, 2, 0, 40) - datetime(2000, 1, 2, 0, 20, 1)
                ).total_seconds()
                * 1000,
            },
        ],
    ),
]


@pytest.mark.integration  # type: ignore[misc]
def test_main(
    get_activity_session_collection_content_without_id: Callable[
        [], List[Dict[str, Union[int, datetime, bool]]]
    ],
    get_materialized_view_content: Callable[[str], List[MaterializedViewSchema]],
    clear_storage: Callable[[], None],
) -> None:

    clear_storage()
    for step in TEST_STEPS:

        tested_module.main(
            user_id=TEST_USER_ID,
            activity_events=step["activity_events"],
            reference_time=datetime(1970, 1, 1),
        )

        actual_collection_data = get_activity_session_collection_content_without_id()

        assert actual_collection_data == step["expected_collection_data"]

        for mv in ["learning_time_sessions_duration_mv"]:
            actual_learning_time_materialized_view_data = list(
                get_materialized_view_content(mv)
            )
            assert (
                actual_learning_time_materialized_view_data
                == step[f"expected_{mv}_data"]
            )

    clear_storage()
