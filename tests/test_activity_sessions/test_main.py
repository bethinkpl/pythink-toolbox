# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=protected-access

from datetime import datetime, timedelta
from typing import List, Dict, Union, Callable, Any, Optional

import freezegun
import pytest
from pytest_mock import MockerFixture
from pythink_toolbox.testing.mocking import transform_function_to_target_string
from pythink_toolbox.testing.parametrization import parametrize, Scenario
import pandas as pd

from chronos import __version__
import chronos.activity_sessions.main as tested_module
from chronos.custom_types import TimeRange
from chronos.activity_sessions.activity_events_source import (
    read_activity_events_between_datetimes,
)
from chronos.storage import schemas
from chronos.storage.mongo_specs import mongo_specs
from chronos.storage.schemas import GenerationsSchema


class MainScenario(Scenario):
    read_last_generation_time_range_end_return: Optional[datetime]
    extract_min_time_when_last_status_failed_from_generations_return: Optional[datetime]
    expected_time_range: TimeRange
    expected_reference_time: datetime


test_scenarios: List[MainScenario] = [
    MainScenario(
        desc="all Nones",
        read_last_generation_time_range_end_return=None,
        extract_min_time_when_last_status_failed_from_generations_return=None,
        expected_time_range=TimeRange(
            start=datetime(2019, 8, 11), end=datetime(2020, 1, 1)
        ),
        expected_reference_time=datetime(2019, 8, 11),
    ),
    MainScenario(
        desc="read_last_generation_time_range_end_return not None",
        read_last_generation_time_range_end_return=datetime(2019, 12, 1),
        extract_min_time_when_last_status_failed_from_generations_return=None,
        expected_time_range=TimeRange(
            start=datetime(2019, 12, 1), end=datetime(2020, 1, 1)
        ),
        expected_reference_time=datetime(2019, 12, 1),
    ),
    MainScenario(
        desc="extract_min_time_when_last_status_failed_from_generations_return not None",
        read_last_generation_time_range_end_return=None,
        extract_min_time_when_last_status_failed_from_generations_return=datetime(
            2019, 9, 8
        ),
        expected_time_range=TimeRange(
            start=datetime(2019, 8, 11), end=datetime(2020, 1, 1)
        ),
        expected_reference_time=datetime(2019, 9, 8),
    ),
    MainScenario(
        desc="both not None",
        read_last_generation_time_range_end_return=datetime(2019, 12, 1),
        extract_min_time_when_last_status_failed_from_generations_return=datetime(
            2019, 9, 8
        ),
        expected_time_range=TimeRange(
            start=datetime(2019, 12, 1), end=datetime(2020, 1, 1)
        ),
        expected_reference_time=datetime(2019, 9, 8),
    ),
]


@freezegun.freeze_time("2020-01-01")
@parametrize(test_scenarios)  # type: ignore[misc]
def test_main(
    mocker: MockerFixture,
    read_last_generation_time_range_end_return: Optional[datetime],
    extract_min_time_when_last_status_failed_from_generations_return: Optional[
        datetime
    ],
    expected_time_range: TimeRange,
    expected_reference_time: datetime,
) -> None:
    """Makes sure that functions are called with proper arguments."""

    mocker.patch(
        "chronos.activity_sessions.main.read_last_generation_time_range_end",
        return_value=read_last_generation_time_range_end_return,
    )

    mocker.patch(
        "chronos.activity_sessions.main.extract_min_time_when_last_status_failed_from_generations",
        return_value=extract_min_time_when_last_status_failed_from_generations_return,
    )

    mocked__run_activity_sessions_generation = mocker.patch(
        transform_function_to_target_string(
            tested_module._run_activity_sessions_generation
        )
    )
    mocked_update_materialized_views = mocker.patch(
        transform_function_to_target_string(
            tested_module.storage_operations.update_materialized_views
        )
    )

    tested_module.main()

    mocked__run_activity_sessions_generation.assert_called_once_with(
        time_range=expected_time_range
    )
    mocked_update_materialized_views.assert_called_once_with(
        reference_time=expected_reference_time
    )


# TODO LACE-465 When GBQ integration ready -> replace mock/add new test
@freezegun.freeze_time("2000-1-2")
@pytest.mark.usefixtures("clear_storage")
@pytest.mark.e2e
@pytest.mark.integration
def test__run_activity_sessions_generation(
    mocker: MockerFixture,
    get_collection_content_without_id_factory: Callable[
        [str], List[Dict[str, Union[int, datetime, bool]]]
    ],
) -> None:
    """End-to-end overall happy-path activity sessions creation and materialized views updates."""

    def mock_side_effect(**kwargs: Any) -> pd.DataFrame:
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

    tested_module._run_activity_sessions_generation(
        time_range=TimeRange(datetime(2000, 1, 1), datetime(2000, 1, 2))
    )

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


class CalculateIntervalsForTimeRangeScenario(Scenario):
    time_range: TimeRange
    expected: List[TimeRange]


test_scenarios: List[CalculateIntervalsForTimeRangeScenario] = [  # type: ignore[no-redef]
    CalculateIntervalsForTimeRangeScenario(
        desc="no range",
        time_range=TimeRange(start=datetime(2000, 1, 1), end=datetime(2000, 1, 1)),
        expected=[],
    ),
    CalculateIntervalsForTimeRangeScenario(
        desc="one range",
        time_range=TimeRange(start=datetime(2000, 1, 1), end=datetime(2000, 1, 31)),
        expected=[
            TimeRange(start=datetime(2000, 1, 1), end=datetime(2000, 1, 31)),
        ],
    ),
    CalculateIntervalsForTimeRangeScenario(
        desc="two ranges",
        time_range=TimeRange(start=datetime(2000, 1, 1), end=datetime(2000, 1, 31, 1)),
        expected=[
            TimeRange(start=datetime(2000, 1, 1), end=datetime(2000, 1, 31)),
            TimeRange(start=datetime(2000, 1, 31), end=datetime(2000, 1, 31, 1)),
        ],
    ),
    CalculateIntervalsForTimeRangeScenario(
        desc="many ranges",
        time_range=TimeRange(start=datetime(2000, 1, 1), end=datetime(2000, 3, 15)),
        expected=[
            TimeRange(start=datetime(2000, 1, 1), end=datetime(2000, 1, 31)),
            TimeRange(start=datetime(2000, 1, 31), end=datetime(2000, 3, 1)),
            TimeRange(start=datetime(2000, 3, 1), end=datetime(2000, 3, 15)),
        ],
    ),
]


@parametrize(test_scenarios)  # type: ignore[misc]
def test__calculate_intervals_for_time_range(
    time_range: TimeRange, expected: List[TimeRange]
) -> None:

    interval_size: timedelta = timedelta(days=30)

    actual = tested_module._calculate_intervals_for_time_range(
        time_range=time_range, interval_size=interval_size
    )

    assert expected == actual


@freezegun.freeze_time("2000-03-04")
def test__save_generation_data(
    get_collection_content_without_id_factory: Callable[
        [str], List[Dict[str, Union[int, datetime, bool]]]
    ],
) -> None:

    time_range = TimeRange(start=datetime(2000, 1, 1), end=datetime(2000, 1, 31))

    with tested_module._save_generation_data(time_range=time_range):
        assert get_collection_content_without_id_factory("generations") == [
            GenerationsSchema(
                time_range={"start": time_range.start, "end": time_range.end},
                start_time=datetime(2000, 3, 4),
                version=__version__,
            )
        ]

    assert get_collection_content_without_id_factory("generations") == [
        GenerationsSchema(
            time_range={"start": time_range.start, "end": time_range.end},
            start_time=datetime(2000, 3, 4),
            end_time=datetime(2000, 3, 4),
            version=__version__,
        )
    ]
