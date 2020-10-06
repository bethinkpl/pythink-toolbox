# pylint: disable=missing-function-docstring

from datetime import datetime, timedelta
from typing import Callable, List, Dict, Union, Iterator

import pytest
import pytest_steps
from pytest_mock import MockerFixture
from pythink_toolbox.testing.mocking import transform_function_to_target_string
import pandas as pd

import chronos
import chronos.activity_sessions.main as tested_module
from chronos.activity_sessions.activity_events_source import (
    read_activity_events_between_datetimes,
)
from chronos.storage.schemas import MaterializedViewSchema


# TODO LACE-465 When GBQ integration ready -> replace mock/add new test
@pytest.mark.e2e  # type: ignore[misc]
@pytest.mark.integration  # type: ignore[misc]
@pytest_steps.test_steps(  # type: ignore[misc]
    "step_test_collection_content",
    "step_test_learning_time_sessions_duration_mv_content",
    "step_test_break_sessions_duration_mv_content",
    "step_test_focus_sessions_duration_mv_content",
)
def test_main(
    mocker: MockerFixture,
    get_activity_session_collection_content_without_id: Callable[
        [], List[Dict[str, Union[int, datetime, bool]]]
    ],
    get_materialized_view_content: Callable[[str], List[MaterializedViewSchema]],
    clear_storage: Callable[[], None],
) -> Iterator[None]:
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

    clear_storage()

    tested_module.main(start_time=datetime(2000, 1, 1), end_time=datetime(2000, 1, 2))

    result_data_1 = get_activity_session_collection_content_without_id()

    expected_data_1 = [
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

    assert result_data_1 == expected_data_1
    yield

    result_data_2 = get_materialized_view_content("learning_time_sessions_duration_mv")
    expected_data_2 = [
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

    assert result_data_2 == expected_data_2
    yield

    result_data_3 = get_materialized_view_content("break_sessions_duration_mv")
    expected_data_3 = [
        {
            "_id": {"user_id": 1, "start_time": datetime(2000, 1, 1, 0, 35)},
            "end_time": datetime(2000, 1, 1, 1),
            "duration_ms": int(
                (datetime(2000, 1, 1, 1) - datetime(2000, 1, 1, 0, 35))
                / timedelta(milliseconds=1)
            ),
        }
    ]

    assert result_data_3 == expected_data_3
    yield

    result_data_4 = get_materialized_view_content("focus_sessions_duration_mv")
    expected_data_4 = [
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

    assert result_data_4 == expected_data_4
    clear_storage()
    yield
