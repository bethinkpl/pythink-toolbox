# pylint: disable=missing-function-docstring

from datetime import datetime, timedelta
from typing import List, Dict, Union, Callable

import freezegun
import pytest
from pytest_mock import MockerFixture
from pythink_toolbox.testing.mocking import transform_function_to_target_string
import pandas as pd

import chronos
import chronos.activity_sessions.main as tested_module
from chronos.custom_types import TimeRange
from chronos.activity_sessions.activity_events_source import (
    read_activity_events_between_datetimes,
)
from chronos.storage import schemas


# TODO LACE-465 When GBQ integration ready -> replace mock/add new test
@freezegun.freeze_time("2000-1-2")  # type: ignore[misc]
@pytest.mark.usefixtures("clear_storage")  # type: ignore[misc]
@pytest.mark.e2e  # type: ignore[misc]
@pytest.mark.integration  # type: ignore[misc]
def test_main(
    mocker: MockerFixture,
    get_collection_content_without_id_factory: Callable[
        [str], List[Dict[str, Union[int, datetime, bool]]]
    ],
    get_materialized_view_content_factory: Callable[
        [str], List[schemas.MaterializedViewSchema]
    ],
) -> None:
    """End-to-end overall happy-path activity sessions creation and materialized views updates."""

    mocker.patch(
        transform_function_to_target_string(read_activity_events_between_datetimes),
        return_value=pd.DataFrame(
            columns=["user_id", "client_time"],
            data=[
                [1, datetime(2000, 1, 1, 0, 1)],
                [1, datetime(2000, 1, 1, 0, 5)],
                [1, datetime(2000, 1, 1, 0, 10)],
                [1, datetime(2000, 1, 1, 0, 15)],
                [1, datetime(2000, 1, 1, 0, 20)],
                [1, datetime(2000, 1, 1, 0, 25)],
                [1, datetime(2000, 1, 1, 0, 30)],
                [1, datetime(2000, 1, 1, 0, 35)],
                [1, datetime(2000, 1, 1, 1, 1)],
                [1, datetime(2000, 1, 1, 1, 5)],
                [1, datetime(2000, 1, 1, 1, 10)],
                [1, datetime(2000, 1, 1, 1, 15)],
                [1, datetime(2000, 1, 1, 1, 20)],
                [1, datetime(2000, 1, 1, 1, 25)],
                [1, datetime(2000, 1, 1, 1, 30)],
                [2, datetime(2000, 1, 1, 0, 1)],
            ],
        ),
    )

    tested_module.main(time_range=TimeRange(datetime(2000, 1, 1), datetime(2000, 1, 2)))

    expected_activity_sessions_data = [
        {
            "user_id": 1,
            "start_time": datetime(2000, 1, 1),
            "end_time": datetime(2000, 1, 1, 0, 35),
            "is_active": True,
            "is_break": False,
            "is_focus": True,
            "version": chronos.__version__,
        },
        {
            "user_id": 1,
            "start_time": datetime(2000, 1, 1, 0, 35),
            "end_time": datetime(2000, 1, 1, 1),
            "is_active": False,
            "is_break": True,
            "is_focus": False,
            "version": chronos.__version__,
        },
        {
            "user_id": 1,
            "start_time": datetime(2000, 1, 1, 1),
            "end_time": datetime(2000, 1, 1, 1, 30),
            "is_active": True,
            "is_break": False,
            "is_focus": True,
            "version": chronos.__version__,
        },
        {
            "user_id": 2,
            "start_time": datetime(2000, 1, 1),
            "end_time": datetime(2000, 1, 1, 0, 1),
            "is_active": True,
            "is_break": False,
            "is_focus": False,
            "version": chronos.__version__,
        },
    ]
    actual_activity_sessions_data = get_collection_content_without_id_factory(
        "activity_sessions"
    )

    assert actual_activity_sessions_data == expected_activity_sessions_data

    expected_learning_time_sessions_duration_mv_data = [
        {
            "_id": {"user_id": 1, "start_time": datetime(2000, 1, 1)},
            "end_time": datetime(2000, 1, 1, 0, 35),
            "duration_ms": int(
                (datetime(2000, 1, 1, 0, 35) - datetime(2000, 1, 1))
                / timedelta(milliseconds=1)
            ),
        },
        {
            "_id": {"user_id": 1, "start_time": datetime(2000, 1, 1, 0, 35)},
            "end_time": datetime(2000, 1, 1, 1),
            "duration_ms": int(
                (datetime(2000, 1, 1, 1) - datetime(2000, 1, 1, 0, 35))
                / timedelta(milliseconds=1)
            ),
        },
        {
            "_id": {"user_id": 1, "start_time": datetime(2000, 1, 1, 1)},
            "end_time": datetime(2000, 1, 1, 1, 30),
            "duration_ms": int(
                (datetime(2000, 1, 1, 1, 30) - datetime(2000, 1, 1, 1))
                / timedelta(milliseconds=1)
            ),
        },
        {
            "_id": {"user_id": 2, "start_time": datetime(2000, 1, 1)},
            "end_time": datetime(2000, 1, 1, 0, 1),
            "duration_ms": int(
                (datetime(2000, 1, 1, 0, 1) - datetime(2000, 1, 1))
                / timedelta(milliseconds=1)
            ),
        },
    ]
    actual_learning_time_sessions_duration_mv_data = (
        get_materialized_view_content_factory("learning_time_sessions_duration_mv")
    )

    assert (
        actual_learning_time_sessions_duration_mv_data
        == expected_learning_time_sessions_duration_mv_data
    )

    expected_break_sessions_duration_mv_data = [
        {
            "_id": {"user_id": 1, "start_time": datetime(2000, 1, 1, 0, 35)},
            "end_time": datetime(2000, 1, 1, 1),
            "duration_ms": int(
                (datetime(2000, 1, 1, 1) - datetime(2000, 1, 1, 0, 35))
                / timedelta(milliseconds=1)
            ),
        }
    ]
    actual_break_sessions_duration_mv_data = get_materialized_view_content_factory(
        "break_sessions_duration_mv"
    )

    assert (
        actual_break_sessions_duration_mv_data
        == expected_break_sessions_duration_mv_data
    )

    expected_focus_sessions_duration_mv_data = [
        {
            "_id": {"user_id": 1, "start_time": datetime(2000, 1, 1)},
            "end_time": datetime(2000, 1, 1, 0, 35),
            "duration_ms": int(
                (datetime(2000, 1, 1, 0, 35) - datetime(2000, 1, 1))
                / timedelta(milliseconds=1)
            ),
        },
        {
            "_id": {"user_id": 1, "start_time": datetime(2000, 1, 1, 1)},
            "end_time": datetime(2000, 1, 1, 1, 30),
            "duration_ms": int(
                (datetime(2000, 1, 1, 1, 30) - datetime(2000, 1, 1, 1))
                / timedelta(milliseconds=1)
            ),
        },
    ]
    actual_focus_sessions_duration_mv_data = get_materialized_view_content_factory(
        "focus_sessions_duration_mv"
    )

    assert (
        actual_focus_sessions_duration_mv_data
        == expected_focus_sessions_duration_mv_data
    )

    expected_generations_data = [
        {
            "time_range": {"start": datetime(2000, 1, 1), "end": datetime(2000, 1, 2)},
            "start_time": datetime(2000, 1, 2),
            "end_time": datetime(2000, 1, 2),
        }
    ]

    assert expected_generations_data == get_collection_content_without_id_factory(
        "generations"
    )
