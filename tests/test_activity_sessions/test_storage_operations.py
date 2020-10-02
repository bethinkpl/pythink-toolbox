"""Whole activity_sessions creation flow for one user."""
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=duplicate-code

from datetime import datetime
from typing import Dict, List, Union, Callable

import pandas as pd
import pytest
import pytest_steps

import chronos
import chronos.activity_sessions.storage_operations as tested_module

from chronos.storage.storage import MaterializedViewSchema

TEST_USER_ID = 108


@pytest.mark.integration  # type: ignore[misc]
@pytest_steps.test_steps(
    [
        "Initial input - creates two separate active sessions and inactive in the middle",
        "Takes last active session & extends its duration.",
        "Takes last active session & extends its duration, so it changes to focus session.",
        "Add new sessions one focused in the end and on that is 'break' before.",
    ]
)
def test_save_new_activity_sessions(
    get_activity_session_collection_content_without_id: Callable[
        [], List[Dict[str, Union[int, datetime, bool]]]
    ],
    get_materialized_view_content: Callable[[str], List[MaterializedViewSchema]],
    clear_storage: Callable[[], None],
) -> None:
    def _save_new_activity_sessions_and_get_its_content(_activity_events: pd.Series):
        tested_module.save_new_activity_sessions(
            user_id=TEST_USER_ID,
            activity_events=_activity_events,
            reference_time=datetime(1970, 1, 1),
        )

        return get_activity_session_collection_content_without_id()

    clear_storage()

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

    actual_collection_content = _save_new_activity_sessions_and_get_its_content(
        _activity_events=activity_events_1
    )

    assert actual_collection_content == expected_collection_content_1
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

    actual_collection_content = _save_new_activity_sessions_and_get_its_content(
        _activity_events=activity_events_2
    )

    assert actual_collection_content == expected_collection_content_2
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

    actual_collection_content = _save_new_activity_sessions_and_get_its_content(
        _activity_events=activity_events_3
    )

    assert actual_collection_content == expected_collection_content_3
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

    actual_collection_content = _save_new_activity_sessions_and_get_its_content(
        _activity_events=activity_events_4
    )

    assert actual_collection_content == expected_collection_content_4

    clear_storage()
    yield

    # expected_materialized_views_content = {
    #     "actual_learning_time_sessions_duration_mv_content": (
    #         [
    #             {
    #                 "_id": {
    #                     "user_id": TEST_USER_ID,
    #                     "start_time": datetime(1999, 12, 31, 23, 59),
    #                 },
    #                 "end_time": datetime(2000, 1, 1, 0, 0),
    #                 "duration_ms": int(
    #                     (
    #                         datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
    #                     ).total_seconds()
    #                     * 1000
    #                 ),
    #             },
    #             {
    #                 "_id": {
    #                     "user_id": TEST_USER_ID,
    #                     "start_time": datetime(2000, 1, 1, 23, 59),
    #                 },
    #                 "end_time": datetime(2000, 1, 2, 0, 0),
    #                 "duration_ms": int(
    #                     (
    #                         datetime(2000, 1, 2, 0, 0) - datetime(2000, 1, 1, 23, 59)
    #                     ).total_seconds()
    #                     * 1000
    #                 ),
    #             },
    #         ],
    #     ),
    #     "actual_break_sessions_duration_mv_content": [],
    #     "actual_focus_sessions_duration_mv_content": [],
    # }
    #
    # actual_materialized_views_content = _get_actual_materialized_views_content(
    #     get_materialized_view_content=get_materialized_view_content
    # )
    #
    # assert actual_materialized_views_content == expected_materialized_views_content
    #
    # yield
    #
    # clear_storage()

    # step1
    # expected_collection_content = (
    #     [
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(1999, 12, 31, 23, 59),
    #             "end_time": datetime(2000, 1, 1, 0, 0),
    #             "is_active": True,
    #             "is_focus": False,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(2000, 1, 1, 0, 0),
    #             "end_time": datetime(2000, 1, 1, 23, 59),
    #             "is_active": False,
    #             "is_focus": False,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(2000, 1, 1, 23, 59),
    #             "end_time": datetime(2000, 1, 2, 0, 0),
    #             "is_active": True,
    #             "is_focus": False,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #     ],
    # )
    # expected_learning_time_sessions_duration_mv_data = (
    #     [
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(1999, 12, 31, 23, 59),
    #             },
    #             "end_time": datetime(2000, 1, 1, 0, 0),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(2000, 1, 1, 23, 59),
    #             },
    #             "end_time": datetime(2000, 1, 2, 0, 0),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 2, 0, 0) - datetime(2000, 1, 1, 23, 59)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #     ],
    # )
    # expected_break_sessions_duration_mv_data = ([],)
    # expected_focus_sessions_duration_mv_data = ([],)

    # step 2
    # expected_collection_content = (
    #     [
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(1999, 12, 31, 23, 59),
    #             "end_time": datetime(2000, 1, 1, 0, 0),
    #             "is_active": True,
    #             "is_focus": False,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(2000, 1, 1, 0, 0),
    #             "end_time": datetime(2000, 1, 1, 23, 59),
    #             "is_active": False,
    #             "is_focus": False,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(2000, 1, 1, 23, 59),
    #             "end_time": datetime(2000, 1, 2, 0, 5),
    #             "is_active": True,
    #             "is_focus": False,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #     ],
    # )
    # expected_learning_time_sessions_duration_mv_data = (
    #     [
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(1999, 12, 31, 23, 59),
    #             },
    #             "end_time": datetime(2000, 1, 1, 0, 0),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(2000, 1, 1, 23, 59),
    #             },
    #             "end_time": datetime(2000, 1, 2, 0, 5),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 2, 0, 5) - datetime(2000, 1, 1, 23, 59)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #     ],
    # )
    # expected_break_sessions_duration_mv_data = ([],)
    # expected_focus_sessions_duration_mv_data = ([],)

    # step 3
    # expected_collection_content = (
    #     [
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(1999, 12, 31, 23, 59),
    #             "end_time": datetime(2000, 1, 1, 0, 0),
    #             "is_active": True,
    #             "is_focus": False,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(2000, 1, 1, 0, 0),
    #             "end_time": datetime(2000, 1, 1, 23, 59),
    #             "is_active": False,
    #             "is_focus": False,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(2000, 1, 1, 23, 59),
    #             "end_time": datetime(2000, 1, 2, 0, 15),
    #             "is_active": True,
    #             "is_focus": True,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #     ],
    # )
    # expected_learning_time_sessions_duration_mv_data = (
    #     [
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(1999, 12, 31, 23, 59),
    #             },
    #             "end_time": datetime(2000, 1, 1, 0, 0),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(2000, 1, 1, 23, 59),
    #             },
    #             "end_time": datetime(2000, 1, 2, 0, 15),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 2, 0, 15) - datetime(2000, 1, 1, 23, 59)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #     ],
    # )
    # expected_break_sessions_duration_mv_data = ([],)
    # expected_focus_sessions_duration_mv_data = (
    #     [
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(2000, 1, 1, 23, 59),
    #             },
    #             "end_time": datetime(2000, 1, 2, 0, 15),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 2, 0, 15) - datetime(2000, 1, 1, 23, 59)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         }
    #     ],
    # )

    # last step
    # expected_collection_content = (
    #     [
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(1999, 12, 31, 23, 59),
    #             "end_time": datetime(2000, 1, 1, 0, 0),
    #             "is_active": True,
    #             "is_focus": False,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(2000, 1, 1, 0, 0),
    #             "end_time": datetime(2000, 1, 1, 23, 59),
    #             "is_active": False,
    #             "is_focus": False,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(2000, 1, 1, 23, 59),
    #             "end_time": datetime(2000, 1, 2, 0, 15),
    #             "is_active": True,
    #             "is_focus": True,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(2000, 1, 2, 0, 15),
    #             "end_time": datetime(2000, 1, 2, 0, 20, 1),
    #             "is_active": False,
    #             "is_focus": False,
    #             "is_break": True,
    #             "version": chronos.__version__,
    #         },
    #         {
    #             "user_id": TEST_USER_ID,
    #             "start_time": datetime(2000, 1, 2, 0, 20, 1),
    #             "end_time": datetime(2000, 1, 2, 0, 40),
    #             "is_active": True,
    #             "is_focus": True,
    #             "is_break": False,
    #             "version": chronos.__version__,
    #         },
    #     ],
    # )
    # expected_learning_time_sessions_duration_mv_data = (
    #     [
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(1999, 12, 31, 23, 59),
    #             },
    #             "end_time": datetime(2000, 1, 1, 0, 0),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 1, 0, 0) - datetime(1999, 12, 31, 23, 59)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(2000, 1, 1, 23, 59),
    #             },
    #             "end_time": datetime(2000, 1, 2, 0, 15),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 2, 0, 15) - datetime(2000, 1, 1, 23, 59)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(2000, 1, 2, 0, 15),
    #             },
    #             "end_time": datetime(2000, 1, 2, 0, 20, 1),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 2, 0, 20, 1) - datetime(2000, 1, 2, 0, 15)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(2000, 1, 2, 0, 20, 1),
    #             },
    #             "end_time": datetime(2000, 1, 2, 0, 40),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 2, 0, 40) - datetime(2000, 1, 2, 0, 20, 1)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #     ],
    # )
    # expected_break_sessions_duration_mv_data = (
    #     [
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(2000, 1, 2, 0, 15),
    #             },
    #             "end_time": datetime(2000, 1, 2, 0, 20, 1),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 2, 0, 20, 1) - datetime(2000, 1, 2, 0, 15)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         }
    #     ],
    # )
    # expected_focus_sessions_duration_mv_data = (
    #     [
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(2000, 1, 1, 23, 59),
    #             },
    #             "end_time": datetime(2000, 1, 2, 0, 15),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 2, 0, 15) - datetime(2000, 1, 1, 23, 59)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #         {
    #             "_id": {
    #                 "user_id": TEST_USER_ID,
    #                 "start_time": datetime(2000, 1, 2, 0, 20, 1),
    #             },
    #             "end_time": datetime(2000, 1, 2, 0, 40),
    #             "duration_ms": int(
    #                 (
    #                     datetime(2000, 1, 2, 0, 40) - datetime(2000, 1, 2, 0, 20, 1)
    #                 ).total_seconds()
    #                 * 1000
    #             ),
    #         },
    #     ],
    # )


def _get_actual_materialized_views_content(get_materialized_view_content):
    materialized_view_names = [
        "learning_time_sessions_duration_mv",
        "break_sessions_duration_mv",
        "focus_sessions_duration_mv",
    ]

    return {
        f"actual_{mv_name}_content": get_materialized_view_content(mv_name)
        for mv_name in materialized_view_names
    }
