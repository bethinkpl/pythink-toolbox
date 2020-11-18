# pylint: disable=missing-function-docstring

from datetime import datetime, timedelta
from typing import List, Dict, Union, Callable

import freezegun
import pytest
from pytest_mock import MockerFixture
from pythink_toolbox.testing.mocking import transform_function_to_target_string
import pandas as pd

from chronos import __version__
import chronos.activity_sessions.main as tested_module
from chronos.custom_types import TimeRange
from chronos.activity_sessions.activity_events_source import (
    read_activity_events_between_datetimes,
)
from chronos.storage import schemas, mongo_specs


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

    def mock_side_effect(**kwargs):
        if "user_exclude" in kwargs:
            assert kwargs["user_exclude"]
            return pd.DataFrame(
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
            )

        return pd.DataFrame(
            columns=["user_id", "client_time"],
            data=[[3, datetime(1999, 12, 31, 1, 1)]],
        )

    mocker.patch(
        transform_function_to_target_string(read_activity_events_between_datetimes),
        side_effect=mock_side_effect,
    )

    mongo_specs.collections.users_generation_statuses.insert_one(
        schemas.UsersGenerationStatuesSchema(
            user_id=3,
            last_status="failed",
            time_until_generations_successful=datetime(1999, 12, 31, 1),
            version=__version__,
        )
    )

    tested_module.main(time_range=TimeRange(datetime(2000, 1, 1), datetime(2000, 1, 2)))

    # ===================================== CHECK =====================================
    expected_activity_sessions_data = [
        {
            "user_id": 1,
            "start_time": datetime(2000, 1, 1),
            "end_time": datetime(2000, 1, 1, 0, 35),
            "is_active": True,
            "is_break": False,
            "is_focus": True,
            "version": __version__,
        },
        {
            "user_id": 1,
            "start_time": datetime(2000, 1, 1, 0, 35),
            "end_time": datetime(2000, 1, 1, 1),
            "is_active": False,
            "is_break": True,
            "is_focus": False,
            "version": __version__,
        },
        {
            "user_id": 1,
            "start_time": datetime(2000, 1, 1, 1),
            "end_time": datetime(2000, 1, 1, 1, 30),
            "is_active": True,
            "is_break": False,
            "is_focus": True,
            "version": __version__,
        },
        {
            "user_id": 2,
            "start_time": datetime(2000, 1, 1),
            "end_time": datetime(2000, 1, 1, 0, 1),
            "is_active": True,
            "is_break": False,
            "is_focus": False,
            "version": __version__,
        },
        {
            "user_id": 3,
            "start_time": datetime(1999, 12, 31, 1),
            "end_time": datetime(1999, 12, 31, 1, 1),
            "is_active": True,
            "is_break": False,
            "is_focus": False,
            "version": __version__,
        },
    ]

    assert (
        get_collection_content_without_id_factory("activity_sessions")
        == expected_activity_sessions_data
    )

    # ===================================== CHECK =====================================
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

    assert (
        get_materialized_view_content_factory("learning_time_sessions_duration_mv")
        == expected_learning_time_sessions_duration_mv_data
    )

    # ===================================== CHECK =====================================
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

    assert (
        get_materialized_view_content_factory("break_sessions_duration_mv")
        == expected_break_sessions_duration_mv_data
    )

    # ===================================== CHECK =====================================
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

    assert (
        get_materialized_view_content_factory("focus_sessions_duration_mv")
        == expected_focus_sessions_duration_mv_data
    )

    # ===================================== CHECK =====================================
    expected_generations_data = [
        {
            "time_range": {"start": datetime(2000, 1, 1), "end": datetime(2000, 1, 2)},
            "start_time": datetime(2000, 1, 2),
            "end_time": datetime(2000, 1, 2),
            "version": __version__,
        }
    ]

    assert expected_generations_data == get_collection_content_without_id_factory(
        "generations"
    )
