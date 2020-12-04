"""Whole activity_sessions creation flow for one user."""
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=duplicate-code
# pylint: disable=protected-access
# pylint: disable=too-many-statements

from datetime import datetime
from typing import Dict, List, Callable, Iterator, Any, Literal

import pandas as pd
import pytest
import pytest_steps
from pythink_toolbox.testing.parametrization import parametrize, Scenario

import chronos.activity_sessions.storage_operations as tested_module
import chronos
import chronos.activity_sessions.generation_operations
from chronos import custom_types
from chronos.storage import schemas

TEST_USER_ID = 108


@pytest.mark.integration
@pytest_steps.test_steps(  # type: ignore[misc]
    "Empty activity events",
    "Initial input - creates two separate active sessions and inactive in the middle",
    "Takes last active session & extends its duration.",
    "Takes last active session & extends its duration, so it changes to focus session.",
    "Add new sessions one focused in the end and on that is 'break' before.",
    "Other user - proper generation.",
)
def test_save_new_activity_sessions(
    get_collection_content_without_id_factory: Callable[[str], List[Dict[str, Any]]],
    clear_storage_factory: Callable[[], None],
) -> Iterator[None]:
    def _save_new_activity_sessions_and_get_its_content(
        _activity_events: pd.Series,
    ) -> List[Dict[str, Any]]:
        tested_module.save_new_activity_sessions(
            user_id=TEST_USER_ID,
            activity_events=_activity_events,
            time_range_end=datetime(1970, 1, 1),
        )

        return get_collection_content_without_id_factory("activity_sessions")

    users_generation_statuses_default_val = [
        {
            "user_id": TEST_USER_ID,
            "last_status": "succeed",
            "time_until_generations_successful": datetime(1970, 1, 1),
            "version": chronos.__version__,
        }
    ]

    clear_storage_factory()

    # ================================= TEST STEP =====================================
    # Empty activity events

    activity_events = pd.Series(dtype="datetime64[ns]")
    expected_collection_content: List[schemas.ActivitySessionSchema] = []

    actual_activity_sessions_collection_content = (
        _save_new_activity_sessions_and_get_its_content(
            _activity_events=activity_events
        )
    )
    assert actual_activity_sessions_collection_content == expected_collection_content

    yield

    # ================================= TEST STEP =====================================
    # Initial input - creates two separate active sessions and inactive in the middle

    activity_events = pd.Series([datetime(2000, 1, 1), datetime(2000, 1, 2)])
    expected_collection_content += [
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
            "end_time": datetime(2000, 1, 2, 0, 0),
            "is_active": True,
            "is_focus": False,
            "is_break": False,
            "version": chronos.__version__,
        },
    ]

    actual_activity_sessions_collection_content = (
        _save_new_activity_sessions_and_get_its_content(
            _activity_events=activity_events
        )
    )
    assert actual_activity_sessions_collection_content == expected_collection_content
    assert (
        get_collection_content_without_id_factory("users_generation_statuses")
        == users_generation_statuses_default_val
    )

    yield

    # ================================= TEST STEP =====================================
    # Takes last active session & extends its duration.

    activity_events = pd.Series([datetime(2000, 1, 2, 0, 5)])
    expected_collection_content[-1]["end_time"] = datetime(2000, 1, 2, 0, 5)

    actual_activity_sessions_collection_content = (
        _save_new_activity_sessions_and_get_its_content(
            _activity_events=activity_events
        )
    )

    assert actual_activity_sessions_collection_content == expected_collection_content
    assert (
        get_collection_content_without_id_factory("users_generation_statuses")
        == users_generation_statuses_default_val
    )

    yield

    # ================================= TEST STEP =====================================
    # Takes last active session & extends its duration, so it changes to focus session.

    activity_events = pd.Series(
        [datetime(2000, 1, 2, 0, 10), datetime(2000, 1, 2, 0, 15)]
    )
    expected_collection_content[-1]["end_time"] = datetime(2000, 1, 2, 0, 15)
    expected_collection_content[-1]["is_focus"] = True

    actual_activity_sessions_collection_content = (
        _save_new_activity_sessions_and_get_its_content(
            _activity_events=activity_events
        )
    )

    assert actual_activity_sessions_collection_content == expected_collection_content
    assert (
        get_collection_content_without_id_factory("users_generation_statuses")
        == users_generation_statuses_default_val
    )

    yield

    # ================================= TEST STEP =====================================
    # Add new sessions one focused in the end and on that is 'break' before.
    activity_events = pd.Series(
        [
            datetime(2000, 1, 2, 0, 21, 1),
            datetime(2000, 1, 2, 0, 25),
            datetime(2000, 1, 2, 0, 30),
            datetime(2000, 1, 2, 0, 35),
            datetime(2000, 1, 2, 0, 40),
        ]
    )
    expected_collection_content += [
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

    actual_activity_sessions_collection_content = (
        _save_new_activity_sessions_and_get_its_content(
            _activity_events=activity_events
        )
    )

    assert actual_activity_sessions_collection_content == expected_collection_content
    assert (
        get_collection_content_without_id_factory("users_generation_statuses")
        == users_generation_statuses_default_val
    )

    yield

    # ================================= TEST STEP =====================================
    # Other user - proper generation.

    tested_module.save_new_activity_sessions(
        user_id=TEST_USER_ID + 1,
        activity_events=pd.Series([datetime(2000, 1, 1)]),
        time_range_end=datetime(2013, 1, 1),
    )
    expected_collection_content += [
        {
            "user_id": TEST_USER_ID + 1,
            "start_time": datetime(1999, 12, 31, 23, 59),
            "end_time": datetime(2000, 1, 1, 0, 0),
            "is_active": True,
            "is_focus": False,
            "is_break": False,
            "version": chronos.__version__,
        }
    ]

    assert (
        get_collection_content_without_id_factory("activity_sessions")
        == expected_collection_content
    )

    clear_storage_factory()
    yield


# =====================================================================================


class UsersGenerationStatusesUpdateScenario(Scenario):
    status: Literal["failed", "succeed"]
    time_range_end: datetime
    expected_users_generation_statuses_content: List[
        schemas.UsersGenerationStatuesSchema
    ]


test_scenarios = [
    UsersGenerationStatusesUpdateScenario(
        desc="status failed",
        status="failed",
        time_range_end=datetime(2000, 1, 1),
        expected_users_generation_statuses_content=[
            schemas.UsersGenerationStatuesSchema(
                user_id=TEST_USER_ID, last_status="failed", version=chronos.__version__
            )
        ],
    ),
    UsersGenerationStatusesUpdateScenario(
        desc="status succeed",
        status="succeed",
        time_range_end=datetime(2000, 1, 1),
        expected_users_generation_statuses_content=[
            schemas.UsersGenerationStatuesSchema(
                user_id=TEST_USER_ID,
                last_status="succeed",
                time_until_generations_successful=datetime(2000, 1, 1),
                version=chronos.__version__,
            )
        ],
    ),
]


@pytest.mark.usefixtures("clear_storage")
@parametrize(test_scenarios)
def test__users_generation_statuses_update(
    get_collection_content_without_id_factory: Callable[[str], List[Dict[str, Any]]],
    status: Literal["failed", "succeed"],
    time_range_end: datetime,
    expected_users_generation_statuses_content: List[
        schemas.UsersGenerationStatuesSchema
    ],
) -> None:

    tested_module._users_generation_statuses_update(
        user_id=TEST_USER_ID, status=status, time_range_end=time_range_end
    )

    assert (
        get_collection_content_without_id_factory("users_generation_statuses")
        == expected_users_generation_statuses_content
    )


# =====================================================================================


def test__return_status():
    @tested_module._return_status
    def _test_func():
        pass

    assert _test_func() == "succeed"

    @tested_module._return_status
    def _test_func_err():
        raise Exception

    assert _test_func_err() == "failed"


# =====================================================================================


class UpdateMaterializedViewsScenario(Scenario):
    activity_sessions_content: List[schemas.ActivitySessionSchema]
    expected_materialized_views_content: Dict[str, List[schemas.MaterializedViewSchema]]


TEST_DATA = [
    UpdateMaterializedViewsScenario(
        desc="",
        activity_sessions_content=[
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
                "end_time": datetime(2000, 1, 2, 0, 0),
                "is_active": True,
                "is_focus": False,
                "is_break": False,
                "version": chronos.__version__,
            },
        ],
        expected_materialized_views_content={
            "learning_time_sessions_duration_mv": [
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(1999, 12, 31, 23, 59),
                    },
                    "end_time": datetime(2000, 1, 1, 0, 0),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
                        ).total_seconds()
                        * 1000
                    ),
                },
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(2000, 1, 1, 23, 59),
                    },
                    "end_time": datetime(2000, 1, 2, 0, 0),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 2, 0, 0) - datetime(2000, 1, 1, 23, 59)
                        ).total_seconds()
                        * 1000
                    ),
                },
            ],
            "break_sessions_duration_mv": [],
            "focus_sessions_duration_mv": [],
        },
    ),
    UpdateMaterializedViewsScenario(
        desc="",
        activity_sessions_content=[
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
                "end_time": datetime(2000, 1, 2, 0, 5),
                "is_active": True,
                "is_focus": False,
                "is_break": False,
                "version": chronos.__version__,
            },
        ],
        expected_materialized_views_content={
            "learning_time_sessions_duration_mv": [
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(1999, 12, 31, 23, 59),
                    },
                    "end_time": datetime(2000, 1, 1, 0, 0),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
                        ).total_seconds()
                        * 1000
                    ),
                },
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(2000, 1, 1, 23, 59),
                    },
                    "end_time": datetime(2000, 1, 2, 0, 5),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 2, 0, 5) - datetime(2000, 1, 1, 23, 59)
                        ).total_seconds()
                        * 1000
                    ),
                },
            ],
            "break_sessions_duration_mv": [],
            "focus_sessions_duration_mv": [],
        },
    ),
    UpdateMaterializedViewsScenario(
        desc="",
        activity_sessions_content=[
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
        ],
        expected_materialized_views_content={
            "learning_time_sessions_duration_mv": [
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(1999, 12, 31, 23, 59),
                    },
                    "end_time": datetime(2000, 1, 1, 0, 0),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
                        ).total_seconds()
                        * 1000
                    ),
                },
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(2000, 1, 1, 23, 59),
                    },
                    "end_time": datetime(2000, 1, 2, 0, 15),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 2, 0, 15) - datetime(2000, 1, 1, 23, 59)
                        ).total_seconds()
                        * 1000
                    ),
                },
            ],
            "break_sessions_duration_mv": [],
            "focus_sessions_duration_mv": [
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(2000, 1, 1, 23, 59),
                    },
                    "end_time": datetime(2000, 1, 2, 0, 15),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 2, 0, 15) - datetime(2000, 1, 1, 23, 59)
                        ).total_seconds()
                        * 1000
                    ),
                }
            ],
        },
    ),
    UpdateMaterializedViewsScenario(
        desc="",
        activity_sessions_content=[
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
        ],
        expected_materialized_views_content={
            "learning_time_sessions_duration_mv": [
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(1999, 12, 31, 23, 59),
                    },
                    "end_time": datetime(2000, 1, 1, 0, 0),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
                        ).total_seconds()
                        * 1000
                    ),
                },
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(2000, 1, 1, 23, 59),
                    },
                    "end_time": datetime(2000, 1, 2, 0, 15),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 2, 0, 15) - datetime(2000, 1, 1, 23, 59)
                        ).total_seconds()
                        * 1000
                    ),
                },
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(2000, 1, 2, 0, 15),
                    },
                    "end_time": datetime(2000, 1, 2, 0, 20, 1),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 2, 0, 20, 1) - datetime(2000, 1, 2, 0, 15)
                        ).total_seconds()
                        * 1000
                    ),
                },
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(2000, 1, 2, 0, 20, 1),
                    },
                    "end_time": datetime(2000, 1, 2, 0, 40),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 2, 0, 40) - datetime(2000, 1, 2, 0, 20, 1)
                        ).total_seconds()
                        * 1000
                    ),
                },
            ],
            "break_sessions_duration_mv": [
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(2000, 1, 2, 0, 15),
                    },
                    "end_time": datetime(2000, 1, 2, 0, 20, 1),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 2, 0, 20, 1) - datetime(2000, 1, 2, 0, 15)
                        ).total_seconds()
                        * 1000
                    ),
                }
            ],
            "focus_sessions_duration_mv": [
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(2000, 1, 1, 23, 59),
                    },
                    "end_time": datetime(2000, 1, 2, 0, 15),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 2, 0, 15) - datetime(2000, 1, 1, 23, 59)
                        ).total_seconds()
                        * 1000
                    ),
                },
                {
                    "_id": {
                        "user_id": TEST_USER_ID,
                        "start_time": datetime(2000, 1, 2, 0, 20, 1),
                    },
                    "end_time": datetime(2000, 1, 2, 0, 40),
                    "duration_ms": int(
                        (
                            datetime(2000, 1, 2, 0, 40) - datetime(2000, 1, 2, 0, 20, 1)
                        ).total_seconds()
                        * 1000
                    ),
                },
            ],
        },
    ),
]


@pytest.mark.usefixtures("clear_storage")
@pytest.mark.integration
@parametrize(TEST_DATA)  # type: ignore[misc]
def test_update_materialized_views(
    activity_sessions_content: List[schemas.ActivitySessionSchema],
    expected_materialized_views_content: Dict[
        str, List[schemas.MaterializedViewSchema]
    ],
    get_materialized_view_content_factory: Callable[
        [str], List[schemas.MaterializedViewSchema]
    ],
    insert_data_to_collection_factory: Callable[
        [str, List[schemas.ActivitySessionSchema]], None
    ],
) -> None:

    insert_data_to_collection_factory("activity_sessions", activity_sessions_content)

    tested_module.update_materialized_views(reference_time=datetime(1970, 1, 1))

    materialized_view_names = [
        "learning_time_sessions_duration_mv",
        "break_sessions_duration_mv",
        "focus_sessions_duration_mv",
    ]

    actual_materialized_views_content = {
        mv_name: get_materialized_view_content_factory(mv_name)
        for mv_name in materialized_view_names
    }

    assert expected_materialized_views_content == actual_materialized_views_content


# =====================================================================================


@pytest.mark.usefixtures("clear_storage")
@pytest.mark.integration
def test_insert_new_generation(
    get_collection_content_without_id_factory: Callable[[str], List[Dict[str, Any]]]
) -> None:

    tested_module.insert_new_generation(
        time_range=custom_types.TimeRange(
            start=datetime(2000, 1, 1), end=datetime(2000, 1, 1, 1)
        ),
        start_time=datetime(2000, 1, 1, 1, 1),
    )
    expected_generations_collection_content = [
        {
            "time_range": {
                "start": datetime(2000, 1, 1),
                "end": datetime(2000, 1, 1, 1),
            },
            "start_time": datetime(2000, 1, 1, 1, 1),
            "version": chronos.__version__,
        },
    ]
    actual_generations_collection_content = get_collection_content_without_id_factory(
        "generations"
    )

    assert (
        actual_generations_collection_content == expected_generations_collection_content
    )


# =====================================================================================


@pytest.mark.usefixtures("clear_storage")
@pytest.mark.integration
def test_update_generation_end_time(
    get_collection_content_without_id_factory: Callable[[str], List[Dict[str, Any]]]
) -> None:

    generation_id = tested_module.insert_new_generation(
        time_range=custom_types.TimeRange(
            start=datetime(2000, 1, 1), end=datetime(2000, 1, 1, 1)
        ),
        start_time=datetime(2000, 1, 1, 1, 1),
    )
    tested_module.update_generation_end_time(
        generation_id=generation_id, end_time=datetime(2000, 1, 1, 1, 2)
    )
    expected_generations_collection_content = [
        {
            "time_range": {
                "start": datetime(2000, 1, 1),
                "end": datetime(2000, 1, 1, 1),
            },
            "start_time": datetime(2000, 1, 1, 1, 1),
            "end_time": datetime(2000, 1, 1, 1, 2),
            "version": chronos.__version__,
        },
    ]
    actual_generations_collection_content = get_collection_content_without_id_factory(
        "generations"
    )

    assert (
        actual_generations_collection_content == expected_generations_collection_content
    )


# =====================================================================================


@pytest.mark.usefixtures("clear_storage")
@pytest.mark.integration
def test_read_last_generation_time_range_end() -> None:

    assert tested_module.read_last_generation_time_range_end() is None

    for i in range(1, 4):
        tested_module.insert_new_generation(
            time_range=custom_types.TimeRange(
                start=datetime(2000, i, i), end=datetime(2000, i, i, i)
            ),
            start_time=datetime(2000, i, i, i, i),
        )

    expected_last_generation_time_range_end = datetime(2000, 3, 3, 3)

    actual_last_generation_time_range_end = (
        tested_module.read_last_generation_time_range_end()
    )

    assert (
        actual_last_generation_time_range_end == expected_last_generation_time_range_end
    )
