"""Whole activity_sessions creation flow for one user."""
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=duplicate-code

from datetime import datetime
from typing import Dict, List, Union, Callable, Iterator, Any

import pandas as pd
import pytest
import pytest_mock
import pytest_steps
from pythink_toolbox.testing import mocking
from pythink_toolbox.testing.parametrization import parametrize, Scenario

import chronos.activity_sessions.storage_operations as tested_module
import chronos
import chronos.activity_sessions.generation_operations
from chronos.storage import mongo_specs, schemas

TEST_USER_ID = 108


@pytest.mark.integration  # type: ignore[misc]
@pytest_steps.test_steps(  # type: ignore[misc]
    "Initial input - creates two separate active sessions and inactive in the middle",
    "Takes last active session & extends its duration.",
    "Takes last active session & extends its duration, so it changes to focus session.",
    "Add new sessions one focused in the end and on that is 'break' before.",
    "Exception raised - check data in failed_generation is correct.",
)
def test_save_new_activity_sessions(
    get_collection_content_without_id_factory: Callable[
        [str], List[Dict[str, Union[int, datetime, bool]]]
    ],
    clear_storage_factory: Callable[[], None],
    mocker: pytest_mock.MockerFixture,
) -> Iterator[None]:
    def _save_new_activity_sessions_and_get_its_content(
        _activity_events: pd.Series,
    ) -> List[Dict[str, Union[int, datetime, bool]]]:
        tested_module.save_new_activity_sessions(
            user_id=TEST_USER_ID,
            activity_events=_activity_events,
            reference_time=datetime(1970, 1, 1),
        )

        return get_collection_content_without_id_factory("activity_sessions")

    clear_storage_factory()

    # Initial input - creates two separate active sessions and inactive in the middle
    activity_events_1 = pd.Series([datetime(2000, 1, 1), datetime(2000, 1, 2)])
    expected_collection_content_1 = [
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
            _activity_events=activity_events_1
        )
    )
    assert actual_activity_sessions_collection_content == expected_collection_content_1
    yield

    # Takes last active session & extends its duration.
    activity_events_2 = pd.Series([datetime(2000, 1, 2, 0, 5)])
    expected_collection_content_2 = [
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
    ]

    actual_activity_sessions_collection_content = (
        _save_new_activity_sessions_and_get_its_content(
            _activity_events=activity_events_2
        )
    )

    assert actual_activity_sessions_collection_content == expected_collection_content_2
    yield

    # Takes last active session & extends its duration, so it changes to focus session.
    activity_events_3 = pd.Series(
        [datetime(2000, 1, 2, 0, 10), datetime(2000, 1, 2, 0, 15)]
    )
    expected_collection_content_3 = [
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
    ]

    actual_activity_sessions_collection_content = (
        _save_new_activity_sessions_and_get_its_content(
            _activity_events=activity_events_3
        )
    )

    assert actual_activity_sessions_collection_content == expected_collection_content_3
    yield

    # Add new sessions one focused in the end and on that is 'break' before.
    activity_events_4 = pd.Series(
        [
            datetime(2000, 1, 2, 0, 21, 1),
            datetime(2000, 1, 2, 0, 25),
            datetime(2000, 1, 2, 0, 30),
            datetime(2000, 1, 2, 0, 35),
            datetime(2000, 1, 2, 0, 40),
        ]
    )
    expected_collection_content_4 = [
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

    actual_activity_sessions_collection_content = (
        _save_new_activity_sessions_and_get_its_content(
            _activity_events=activity_events_4
        )
    )

    assert actual_activity_sessions_collection_content == expected_collection_content_4
    yield

    # Exception raised - check data in failed_generation is correct.

    generate_user_activity_sessions_mock = mocker.patch(
        mocking.transform_function_to_target_string(
            chronos.activity_sessions.generation_operations.generate_user_activity_sessions
        )
    )

    generate_user_activity_sessions_mock.side_effect = Exception("mocked error")

    with pytest.raises(Exception):
        tested_module.save_new_activity_sessions(
            user_id=TEST_USER_ID,
            activity_events=mocker.ANY,
            reference_time=datetime(2013, 1, 1),
        )

        assert get_collection_content_without_id_factory(
            "failed_generation_collection"
        ) == [{"user_id": TEST_USER_ID, "reference_time": datetime(2013, 1, 1)}]

        assert (
            get_collection_content_without_id_factory("activity_sessions")
            == expected_collection_content_4
        )

    clear_storage_factory()
    yield


# =====================================================================================


class UpdateMaterializedViewsScenario(Scenario):
    activity_sessions_content: List[schemas.UserGenerationFailedSchema]
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


@pytest.mark.usefixtures("clear_storage")  # type: ignore[misc]
@pytest.mark.integration  # type: ignore[misc]
@parametrize(TEST_DATA)  # type: ignore[misc]
def test_update_materialized_views(
    activity_sessions_content: List[schemas.UserGenerationFailedSchema],
    expected_materialized_views_content: Dict[
        str, List[schemas.MaterializedViewSchema]
    ],
    get_materialized_view_content_factory,
    insert_data_to_activity_sessions_collection_factory,
) -> None:

    insert_data_to_activity_sessions_collection_factory(activity_sessions_content)

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


@pytest.mark.usefixtures("clear_storage")  # type: ignore[misc]
@pytest.mark.integration  # type: ignore[misc]
def test_insert_new_generation(
    get_collection_content_without_id_factory: Callable[[str], List[Dict[str, Any]]]
) -> None:

    tested_module.insert_new_generation(
        time_range=tested_module.TimeRange(
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
        },
    ]
    actual_generations_collection_content = get_collection_content_without_id_factory(
        "generations"
    )

    assert (
        actual_generations_collection_content == expected_generations_collection_content
    )


@pytest.mark.usefixtures("clear_storage")  # type: ignore[misc]
@pytest.mark.integration  # type: ignore[misc]
def test_update_generation_end_time(
    get_collection_content_without_id_factory: Callable[[str], List[Dict[str, Any]]]
) -> None:

    generation_id = tested_module.insert_new_generation(
        time_range=tested_module.TimeRange(
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
        },
    ]
    actual_generations_collection_content = get_collection_content_without_id_factory(
        "generations"
    )

    assert (
        actual_generations_collection_content == expected_generations_collection_content
    )


@pytest.mark.usefixtures("clear_storage")  # type: ignore[misc]
@pytest.mark.integration  # type: ignore[misc]
def test_read_last_generation_time_range_end(
    get_collection_content_without_id_factory: Callable[[str], List[Dict[str, Any]]]
) -> None:

    for i in range(1, 4):
        tested_module.insert_new_generation(
            time_range=tested_module.TimeRange(
                start=datetime(2000, i, i), end=datetime(2000, i, i, i)
            ),
            start_time=datetime(2000, i, i, i, i),
        )

    expected_generations_collection_content = {
        "time_range": {"end": datetime(2000, 3, 3, 3)}
    }

    actual_generations_collection_content = (
        tested_module.read_last_generation_time_range_end()
    )

    assert (
        actual_generations_collection_content == expected_generations_collection_content
    )
