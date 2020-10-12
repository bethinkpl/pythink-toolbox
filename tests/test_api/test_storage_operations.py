from datetime import datetime, timedelta
from typing import Tuple, List

import pytest
from pythink_toolbox.testing.parametrization import parametrize, Scenario

import chronos.api.storage_operations as tested_module
import chronos
import chronos.activity_sessions.storage_operations
import chronos.settings
from chronos.storage.specs import mongodb

TEST_USER_ID = 1


@pytest.fixture(scope="module")
def clear_storage():
    mongodb.client.drop_database(chronos.settings.MONGO_DATABASE)
    yield
    mongodb.client.drop_database(chronos.settings.MONGO_DATABASE)


@pytest.fixture(scope="module")
def populate_materialized_views_with_test_data():

    activity_sessions = [
        {
            "user_id": TEST_USER_ID,
            "start_time": datetime(1999, 12, 31, 23, 59),
            "end_time": datetime(2000, 1, 1, 0, 0),
            "is_active": True,
            "is_focus": False,
            "is_break": False,
            "version": chronos.__version__,
        },
        {
            "user_id": TEST_USER_ID,
            "start_time": datetime(2000, 1, 1, 0, 0),
            "end_time": datetime(2000, 1, 1, 23, 59),
            "is_active": False,
            "is_focus": False,
            "is_break": False,
            "version": chronos.__version__,
        },
        {
            "user_id": TEST_USER_ID,
            "start_time": datetime(2000, 1, 1, 23, 59),
            "end_time": datetime(2000, 1, 2, 0, 15),
            "is_active": True,
            "is_focus": True,
            "is_break": False,
            "version": chronos.__version__,
        },
        {
            "user_id": TEST_USER_ID,
            "start_time": datetime(2000, 1, 2, 0, 15),
            "end_time": datetime(2000, 1, 2, 0, 20, 1),
            "is_active": False,
            "is_focus": False,
            "is_break": True,
            "version": chronos.__version__,
        },
        {
            "user_id": TEST_USER_ID,
            "start_time": datetime(2000, 1, 2, 0, 20, 1),
            "end_time": datetime(2000, 1, 2, 0, 40),
            "is_active": True,
            "is_focus": True,
            "is_break": False,
            "version": chronos.__version__,
        },
    ]

    mongodb.collections.activity_sessions.insert_many(activity_sessions)

    chronos.activity_sessions.storage_operations.update_materialized_views(
        reference_time=datetime(1970, 1, 1)
    )


class ReadTimeScenario(Scenario):
    time_range: Tuple[datetime, datetime]
    expected_output: List[tested_module.UserDailyTime]


@pytest.mark.integration
@pytest.mark.usefixtures("clear_storage", "populate_materialized_views_with_test_data")
@parametrize(
    [
        ReadTimeScenario(
            desc="all data",
            time_range=(datetime(1970, 1, 1), datetime.now()),
            expected_output=[
                {
                    "date": "1999-12-31",
                    "duration_ms": timedelta(minutes=1) / timedelta(milliseconds=1),
                },
                {
                    "date": "2000-01-01",
                    "duration_ms": timedelta(minutes=16) / timedelta(milliseconds=1),
                },
                {
                    "date": "2000-01-02",
                    "duration_ms": timedelta(minutes=25) / timedelta(milliseconds=1),
                },
            ],
        ),
        ReadTimeScenario(
            desc="data out of range -> empty",
            time_range=(datetime(1970, 1, 1), datetime(1970, 1, 2)),
            expected_output=[],
        ),
        ReadTimeScenario(
            desc="limited range",
            time_range=(datetime(2000, 1, 1, 23, 59, 1), datetime.now()),
            expected_output=[
                {
                    "date": "2000-01-01",
                    "duration_ms": timedelta(minutes=16) / timedelta(milliseconds=1),
                },
                {
                    "date": "2000-01-02",
                    "duration_ms": timedelta(minutes=25) / timedelta(milliseconds=1),
                },
            ],
        ),
    ]
)
def test_read_daily_learning_time(
    time_range: Tuple[datetime, datetime],
    expected_output: List[tested_module.UserDailyTime],
) -> None:
    actual_output = tested_module.read_daily_learning_time(
        user_id=TEST_USER_ID, time_range=time_range
    )

    assert expected_output == actual_output


@pytest.mark.integration
@pytest.mark.usefixtures("clear_storage", "populate_materialized_views_with_test_data")
@parametrize(
    [
        ReadTimeScenario(
            desc="all data",
            time_range=(datetime(1970, 1, 1), datetime.now()),
            expected_output=[
                {
                    "date": "2000-01-02",
                    "duration_ms": int(
                        timedelta(minutes=5, seconds=1) / timedelta(milliseconds=1)
                    ),
                },
            ],
        ),
        ReadTimeScenario(
            desc="data out of range -> empty",
            time_range=(datetime(1970, 1, 1), datetime(1970, 1, 2)),
            expected_output=[],
        ),
        ReadTimeScenario(
            desc="limited range",
            time_range=(datetime(2000, 1, 2, 0, 16), datetime.now()),
            expected_output=[
                {
                    "date": "2000-01-02",
                    "duration_ms": int(
                        timedelta(minutes=5, seconds=1) / timedelta(milliseconds=1)
                    ),
                },
            ],
        ),
    ]
)
def test_read_daily_break_time(
    time_range: Tuple[datetime, datetime],
    expected_output: List[tested_module.UserDailyTime],
) -> None:

    actual_output = tested_module.read_daily_break_time(
        user_id=TEST_USER_ID, time_range=time_range
    )

    assert expected_output == actual_output


@pytest.mark.integration
@pytest.mark.usefixtures("clear_storage", "populate_materialized_views_with_test_data")
@parametrize(
    [
        ReadTimeScenario(
            desc="all data",
            time_range=(datetime(1970, 1, 1), datetime.now()),
            expected_output=[
                {
                    "date": "2000-01-01",
                    "duration_ms": int(
                        timedelta(minutes=16) / timedelta(milliseconds=1)
                    ),
                },
                {
                    "date": "2000-01-02",
                    "duration_ms": int(
                        timedelta(minutes=19, seconds=59) / timedelta(milliseconds=1)
                    ),
                },
            ],
        ),
        ReadTimeScenario(
            desc="data out of range -> empty",
            time_range=(datetime(1970, 1, 1), datetime(1970, 1, 2)),
            expected_output=[],
        ),
        ReadTimeScenario(
            desc="limited range",
            time_range=(datetime(2000, 1, 2), datetime.now()),
            expected_output=[
                {
                    "date": "2000-01-01",
                    "duration_ms": int(
                        timedelta(minutes=16) / timedelta(milliseconds=1)
                    ),
                },
                {
                    "date": "2000-01-02",
                    "duration_ms": int(
                        timedelta(minutes=19, seconds=59) / timedelta(milliseconds=1)
                    ),
                },
            ],
        ),
    ]
)
def test_read_daily_focus_time(
    time_range: Tuple[datetime, datetime],
    expected_output: List[tested_module.UserDailyTime],
) -> None:

    actual_output = tested_module.read_daily_focus_time(
        user_id=TEST_USER_ID, time_range=time_range
    )

    assert expected_output == actual_output


class CumulativeReadTimeScenario(ReadTimeScenario):
    expected_output: int


@pytest.mark.integration
@pytest.mark.usefixtures("clear_storage", "populate_materialized_views_with_test_data")
@parametrize(
    [
        CumulativeReadTimeScenario(
            desc="all data",
            time_range=(datetime(1970, 1, 1), datetime.now()),
            expected_output=int(
                (timedelta(minutes=1) + timedelta(minutes=16) + timedelta(minutes=25))
                / timedelta(milliseconds=1)
            ),
        ),
        CumulativeReadTimeScenario(
            desc="data out of range -> empty",
            time_range=(datetime(1970, 1, 1), datetime(1970, 1, 2)),
            expected_output=0,
        ),
        CumulativeReadTimeScenario(
            desc="limited range",
            time_range=(datetime(2000, 1, 1, 23, 59, 1), datetime.now()),
            expected_output=int(
                (timedelta(minutes=16) + timedelta(minutes=25))
                / timedelta(milliseconds=1)
            ),
        ),
    ]
)
def test_read_cumulative_learning_time(
    time_range: Tuple[datetime, datetime],
    expected_output: List[tested_module.UserDailyTime],
) -> None:
    actual_output = tested_module.read_cumulative_learning_time(
        user_id=TEST_USER_ID, time_range=time_range
    )

    assert expected_output == actual_output
