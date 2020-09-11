# pylint: disable=missing-function-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=protected-access
from datetime import datetime
from typing import Optional

import hypothesis
from hypothesis.extra.pandas import series, indexes, data_frames, column
import pandas as pd  # type: ignore[import]
import pandera.errors  # type: ignore[import]
import pytest
from pythink_toolbox.testing import parametrization

import chronos.create_activity_sessions as tested_module


@pytest.mark.skip(reason="implement when MongoDB I/O operations done")  # type: ignore[misc]
def test_main() -> None:
    assert False


def test__create_user_activity_sessions() -> None:
    """Testing only happy path.
    Precise testing is made in component functions tests."""

    user_id = 1
    activity_events = pd.Series([datetime(2000, 1, 1, 0, 2)], name="client_time")
    last_active_session = pd.DataFrame(
        {"start_time": [datetime(2000, 1, 1)], "end_time": [datetime(2000, 1, 1, 0, 1)]}
    )

    output = tested_module._create_user_activity_sessions(
        user_id=user_id,
        activity_events=activity_events,
        last_active_session=last_active_session,
    )

    assert output == [
        {
            "user_id": 1,
            "start_time": datetime(2000, 1, 1),
            "end_time": datetime(2000, 1, 1, 0, 2),
            "is_active": True,
            "is_focus": False,
            "is_break": False,
        }
    ]


@hypothesis.given(
    activity_events=series(dtype="datetime64[ns]", index=indexes(dtype=int))
)
def test__initialize_sessions_creation(activity_events: pd.Series) -> None:

    if activity_events.empty:
        try:
            tested_module._initialize_sessions_creation(activity_events=activity_events)
        except AssertionError:
            assert True
            return

    output = tested_module._initialize_sessions_creation(
        activity_events=activity_events
    )

    assert isinstance(output, pd.DataFrame)

    pd.testing.assert_series_equal(
        (output.end_time - pd.Timedelta(minutes=1)),
        output.start_time,
        check_names=False,
    )

    pd.testing.assert_frame_equal(output.sort_values("start_time"), output)

    assert output.columns.to_list() == ["start_time", "end_time"]

    assert pd.api.types.is_datetime64_ns_dtype(output.start_time)
    assert pd.api.types.is_datetime64_ns_dtype(output.end_time)

    assert len(output) == len(activity_events)


class AddLastActiveSessionScenario(parametrization.Scenario):
    initialized_sessions: pd.DataFrame
    last_active_session: Optional[pd.DataFrame]
    raise_assertion_error: bool
    expected_output: pd.DataFrame


TEST_SCENARIOS = [
    AddLastActiveSessionScenario(
        desc="empty initialized_sessions",
        initialized_sessions=pd.DataFrame(),
        last_active_session=None,
        raise_assertion_error=True,
        expected_output=pd.DataFrame(),
    ),
    AddLastActiveSessionScenario(
        desc="last_active_session is None",
        initialized_sessions=pd.DataFrame(
            columns=["start_time", "end_time"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1)]],
        ),
        last_active_session=None,
        raise_assertion_error=False,
        expected_output=pd.DataFrame(
            columns=["start_time", "end_time"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1)]],
        ),
    ),
    AddLastActiveSessionScenario(
        desc="last_active_session=None",
        initialized_sessions=pd.DataFrame(
            columns=["start_time", "end_time"],
            data=[[datetime(2000, 2, 1), datetime(2000, 2, 12)]],
        ),
        last_active_session=pd.DataFrame(
            columns=["start_time", "end_time"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1)]],
        ),
        raise_assertion_error=False,
        expected_output=pd.DataFrame(
            columns=["start_time", "end_time"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1)],
                [datetime(2000, 2, 1), datetime(2000, 2, 12)],
            ],
        ),
    ),
]


@parametrization.parametrize(TEST_SCENARIOS)  # type: ignore[misc]
def test__add_last_active_session(
    initialized_sessions: pd.DataFrame,
    last_active_session: Optional[pd.DataFrame],
    raise_assertion_error: bool,
    expected_output: pd.DataFrame,
) -> None:

    if raise_assertion_error:
        with pytest.raises(AssertionError):
            tested_module._add_last_active_session(
                initialized_sessions=initialized_sessions,
                last_active_session=last_active_session,
            )
    else:
        output = tested_module._add_last_active_session(
            initialized_sessions=initialized_sessions,
            last_active_session=last_active_session,
        )

        pd.testing.assert_frame_equal(output, expected_output)


class CreateActiveSessionsScenario(parametrization.Scenario):
    sessions_with_last_active: pd.DataFrame
    raise_assertion_error: bool
    expected_output: pd.DataFrame


TEST_SCENARIOS = [
    CreateActiveSessionsScenario(  # type: ignore[list-item]
        desc="sessions_with_last_active empty -> raise error",
        sessions_with_last_active=pd.DataFrame(),
        raise_assertion_error=True,
        expected_output=pd.DataFrame(),  # value not relevant
    ),
    CreateActiveSessionsScenario(  # type: ignore[list-item]
        desc="only one record in sessions_with_last_active"
        " -> return the the same frame with additional is_active column",
        sessions_with_last_active=pd.DataFrame(
            columns=["start_time", "end_time"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1)]],
        ),
        raise_assertion_error=False,
        expected_output=pd.DataFrame(
            columns=["start_time", "end_time", "is_active"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1), True]],
        ),
    ),
    CreateActiveSessionsScenario(  # type: ignore[list-item]
        desc="Checking how time difference between records influence sessions creation.",
        sessions_with_last_active=pd.DataFrame(
            columns=["start_time", "end_time"],
            data=[
                # -> id 1
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 0, 1)],
                # ->id 1 (diff = 5min)
                [datetime(2000, 1, 1, 0, 6), datetime(2000, 1, 1, 0, 6, 2)],
                # -> id 2 (diff = 5min1s)
                [datetime(2000, 1, 1, 0, 11, 3), datetime(2000, 1, 1, 0, 11, 4)],
                # -> id 2 (diff = 1ns)
                [datetime(2000, 1, 1, 0, 11, 4, 1), datetime(2000, 1, 1, 0, 11, 4, 2)],
            ],
        ),
        raise_assertion_error=False,
        expected_output=pd.DataFrame(
            columns=["start_time", "end_time", "is_active"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 0, 6, 2), True],
                [
                    datetime(2000, 1, 1, 0, 11, 3),
                    datetime(2000, 1, 1, 0, 11, 4, 2),
                    True,
                ],
            ],
        ),
    ),
]


@parametrization.parametrize(TEST_SCENARIOS)  # type: ignore[misc]
def test__create_active_sessions(
    sessions_with_last_active: pd.DataFrame,
    raise_assertion_error: bool,
    expected_output: pd.DataFrame,
) -> None:

    if raise_assertion_error:
        with pytest.raises(AssertionError):
            tested_module._create_active_sessions(
                sessions_with_last_active=sessions_with_last_active
            )
    else:
        output = tested_module._create_active_sessions(
            sessions_with_last_active=sessions_with_last_active
        )

        pd.testing.assert_frame_equal(output, expected_output)


class FillWithInactiveSessionsScenario(parametrization.Scenario):
    active_sessions: pd.DataFrame
    raise_assertion_error: bool
    expected_output: pd.DataFrame


TEST_SCENARIOS = [
    FillWithInactiveSessionsScenario(  # type: ignore[list-item]
        desc="active_sessions is empty -> raise error",
        active_sessions=pd.DataFrame(),
        raise_assertion_error=True,
        expected_output=pd.DataFrame(),  # value not relevant
    ),
    FillWithInactiveSessionsScenario(  # type: ignore[list-item]
        desc="only one active session -> return the same",
        active_sessions=pd.DataFrame(
            columns=["start_time", "end_time", "is_active"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1), True]],
        ),
        raise_assertion_error=False,
        expected_output=pd.DataFrame(
            columns=["start_time", "end_time", "is_active"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1), True]],
        ),
    ),
    FillWithInactiveSessionsScenario(  # type: ignore[list-item]
        desc="three active sessions -> return two inactive sessions in between",
        active_sessions=pd.DataFrame(
            columns=["start_time", "end_time", "is_active"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 0, 6, 2), True],
                [datetime(2000, 1, 1, 0, 11, 3), datetime(2000, 1, 1, 0, 11, 4), True],
                [datetime(2000, 2, 1, 0, 11, 3), datetime(2000, 2, 1, 0, 11, 4), True],
            ],
        ),
        raise_assertion_error=False,
        expected_output=pd.DataFrame(
            columns=["start_time", "end_time", "is_active"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 0, 6, 2), True],
                [datetime(2000, 1, 1, 0, 6, 2), datetime(2000, 1, 1, 0, 11, 3), False],
                [datetime(2000, 1, 1, 0, 11, 3), datetime(2000, 1, 1, 0, 11, 4), True],
                [datetime(2000, 1, 1, 0, 11, 4), datetime(2000, 2, 1, 0, 11, 3), False],
                [datetime(2000, 2, 1, 0, 11, 3), datetime(2000, 2, 1, 0, 11, 4), True],
            ],
        ),
    ),
]


@parametrization.parametrize(TEST_SCENARIOS)  # type: ignore[misc]
def test__fill_with_inactive_sessions(
    active_sessions: pd.DataFrame,
    raise_assertion_error: bool,
    expected_output: pd.DataFrame,
) -> None:

    if raise_assertion_error:
        with pytest.raises(AssertionError):
            tested_module._fill_with_inactive_sessions(active_sessions=active_sessions)
    else:
        output = tested_module._fill_with_inactive_sessions(
            active_sessions=active_sessions
        )

        pd.testing.assert_frame_equal(output, expected_output)


@hypothesis.given(
    active_and_inactive_sessions=data_frames(
        [
            column("start_time", dtype="datetime64[ns]"),
            column("end_time", dtype="datetime64[ns]"),
            column("is_active", dtype=bool),
        ]
    )
)
def test__determine_if_focus(active_and_inactive_sessions: pd.DataFrame) -> None:

    if active_and_inactive_sessions.empty:
        try:
            tested_module._initialize_sessions_creation(activity_events=activity_events)
        except AssertionError:
            assert True
            return

    hypothesis.assume(
        not any(
            active_and_inactive_sessions.start_time
            > active_and_inactive_sessions.end_time
        )
    )
    hypothesis.assume(
        not any(pd.Timestamp(0) > active_and_inactive_sessions.end_time)
        and not any(active_and_inactive_sessions.end_time > datetime(2200, 1, 1))
    )
    hypothesis.assume(
        not any(pd.Timestamp(0) > active_and_inactive_sessions.start_time)
        and not any(active_and_inactive_sessions.start_time > datetime(2200, 1, 1))
    )

    output = tested_module._determine_if_focus(
        active_and_inactive_sessions=active_and_inactive_sessions
    )
    assert isinstance(output, pd.DataFrame)

    pd.testing.assert_series_equal(
        (
            active_and_inactive_sessions.end_time
            - active_and_inactive_sessions.start_time
        ),
        output.duration,
        check_names=False,
    )

    pd.testing.assert_series_equal(
        ((output.duration > pd.Timedelta(minutes=15)) & output.is_active),
        output.is_focus,
        check_names=False,
    )

    assert output.columns.to_list() == [
        "start_time",
        "end_time",
        "is_active",
        "duration",
        "is_focus",
    ]

    assert pd.api.types.is_datetime64_ns_dtype(output.start_time)
    assert pd.api.types.is_datetime64_ns_dtype(output.end_time)
    assert pd.api.types.is_bool_dtype(output.is_active)
    assert pd.api.types.is_timedelta64_ns_dtype(output.duration)
    assert pd.api.types.is_bool_dtype(output.is_focus)

    assert len(output) == len(active_and_inactive_sessions)


@hypothesis.given(
    activity_sessions_with_focus=data_frames(
        [
            column("start_time", dtype="datetime64[ns]"),
            column("end_time", dtype="datetime64[ns]"),
            column("is_active", dtype=bool),
        ]
    )
)
def test__determine_if_break(activity_sessions_with_focus: pd.DataFrame) -> None:

    if activity_sessions_with_focus.empty:
        try:
            tested_module._initialize_sessions_creation(activity_events=activity_events)
        except AssertionError:
            assert True
            return

    hypothesis.assume(
        not any(
            activity_sessions_with_focus.start_time
            > activity_sessions_with_focus.end_time
        )
    )
    hypothesis.assume(
        not any(pd.Timestamp(0) > activity_sessions_with_focus.end_time)
        and not any(activity_sessions_with_focus.end_time > datetime(2100, 1, 1))
    )
    hypothesis.assume(
        not any(pd.Timestamp(0) > activity_sessions_with_focus.start_time)
        and not any(activity_sessions_with_focus.start_time > datetime(2100, 1, 1))
    )

    activity_sessions_with_focus = activity_sessions_with_focus.assign(
        duration=lambda df: df.end_time.sub(df.start_time),
        is_focus=lambda df: df.is_active & df.duration.ge(pd.Timedelta(minutes=15)),
    )

    output = tested_module._determine_if_break(
        activity_sessions_with_focus=activity_sessions_with_focus
    )
    assert isinstance(output, pd.DataFrame)

    for row in output.itertuples():
        if not row.Index:
            prev_row = output.iloc[row.Index - 1]
        else:
            prev_row = pd.Series({"is_focus": False})
        try:
            next_row = output.iloc[row.Index + 1]
        except IndexError:
            next_row = pd.Series({"is_focus": False})
        if row.is_break:
            assert (
                prev_row.is_focus
                and next_row.is_focus
                and (row.end_time - row.start_time) <= pd.Timedelta(minutes=30)
                and not row.is_active
            )
        else:
            assert (
                not prev_row.is_focus
                or not next_row.is_focus
                or not ((row.end_time - row.start_time) <= pd.Timedelta(minutes=30))
                or row.is_active
            )

    assert output.columns.to_list() == [
        "start_time",
        "end_time",
        "is_active",
        "is_focus",
        "is_break",
    ]

    assert pd.api.types.is_datetime64_ns_dtype(output.start_time)
    assert pd.api.types.is_datetime64_ns_dtype(output.end_time)
    assert pd.api.types.is_bool_dtype(output.is_active)
    assert pd.api.types.is_bool_dtype(output.is_focus)
    assert pd.api.types.is_bool_dtype(output.is_break)

    assert len(output) == len(activity_sessions_with_focus)


class SessionsValidationScenario(parametrization.Scenario):
    activity_sessions: pd.DataFrame
    should_pass: bool


TEST_SCENARIOS = [
    SessionsValidationScenario(  # type: ignore[list-item]
        desc="Everything good, this should pass.",
        activity_sessions=pd.DataFrame(
            columns=["start_time", "end_time", "is_active", "is_focus", "is_break"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 1), True, False, False],
                [datetime(2000, 1, 1, 1), datetime(2000, 1, 2), False, False, False],
                [datetime(2000, 1, 2), datetime(2000, 1, 2, 0, 15), True, True, False],
                [
                    datetime(2000, 1, 2, 0, 15),
                    datetime(2000, 1, 2, 0, 45),
                    False,
                    False,
                    True,
                ],
                [
                    datetime(2000, 1, 2, 0, 45),
                    datetime(2000, 1, 2, 1, 15),
                    True,
                    True,
                    False,
                ],
            ],
        ),
        should_pass=True,
    ),
    SessionsValidationScenario(  # type: ignore[list-item]
        desc="Empty DataFrame",
        activity_sessions=pd.DataFrame(
            columns=[
                "start_time",
                "end_time",
                "is_active",
                "is_focus",
                "is_break",
            ]
        ),
        should_pass=False,
    ),
    SessionsValidationScenario(  # type: ignore[list-item]
        desc="First row/last row not active",
        activity_sessions=pd.DataFrame(
            {
                "session_start": [datetime(2000, 1, 1)],
                "session_end": [datetime(2000, 1, 2)],
                "is_active": [False],
                "is_focus": [False],
                "is_break": [False],
            }
        ),
        should_pass=False,
    ),
    SessionsValidationScenario(  # type: ignore[list-item]
        desc="Duplicate session_start",
        activity_sessions=pd.DataFrame(
            columns=["start_time", "end_time", "is_active", "is_focus", "is_break"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 1), True, False, False],
                [datetime(2000, 1, 1, 1), datetime(2000, 1, 2), False, False, False],
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 1), True, False, False],
            ],
        ),
        should_pass=False,
    ),
    SessionsValidationScenario(  # type: ignore[list-item]
        desc="Not sorted",
        activity_sessions=pd.DataFrame(
            columns=["start_time", "end_time", "is_active", "is_focus", "is_break"],
            data=[
                [datetime(2000, 1, 2), datetime(2000, 1, 2, 0, 15), True, True, False],
                [datetime(2000, 1, 1, 1), datetime(2000, 1, 2), False, False, False],
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 1), True, False, False],
            ],
        ),
        should_pass=False,
    ),
    SessionsValidationScenario(  # type: ignore[list-item]
        desc="Wrong inactive sessions duration.",
        activity_sessions=pd.DataFrame(
            columns=["start_time", "end_time", "is_active", "is_focus", "is_break"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 1), True, False, False],
                [
                    datetime(2000, 1, 1, 1),
                    datetime(2000, 1, 1, 0, 1),
                    False,
                    False,
                    False,
                ],
                [
                    datetime(2000, 1, 1, 0, 1),
                    datetime(2000, 1, 2, 0, 15),
                    True,
                    True,
                    False,
                ],
            ],
        ),
        should_pass=False,
    ),
    SessionsValidationScenario(  # type: ignore[list-item]
        desc="Wrong break sessions duration.",
        activity_sessions=pd.DataFrame(
            columns=["start_time", "end_time", "is_active", "is_focus", "is_break"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 1), True, False, False],
                [datetime(2000, 1, 1, 1), datetime(2000, 1, 2), False, False, True],
                [datetime(2000, 1, 2), datetime(2000, 1, 2, 0, 15), True, True, False],
            ],
        ),
        should_pass=False,
    ),
    SessionsValidationScenario(  # type: ignore[list-item]
        desc="end_time != next start_time.",
        activity_sessions=pd.DataFrame(
            columns=["start_time", "end_time", "is_active", "is_focus", "is_break"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 1), True, False, False],
                [datetime(2000, 1, 1, 2), datetime(2000, 1, 2), False, False, False],
                [datetime(2000, 1, 2), datetime(2000, 1, 2, 0, 15), True, True, False],
            ],
        ),
        should_pass=False,
    ),
]


@parametrization.parametrize(TEST_SCENARIOS)  # type: ignore[misc]
def test__sessions_validation(
    activity_sessions: pd.DataFrame, should_pass: bool
) -> None:
    if not should_pass:
        with pytest.raises(pandera.errors.SchemaError):
            tested_module._sessions_validation(activity_sessions=activity_sessions)
    else:
        tested_module._sessions_validation(activity_sessions=activity_sessions)


def test__to_dict() -> None:
    activity_sessions = pd.DataFrame(
        columns=["start_time", "end_time", "is_active", "is_focus", "is_break"],
        data=[
            [datetime(2000, 1, 1), datetime(2000, 1, 1, 1), True, False, False],
            [datetime(2000, 1, 1, 1), datetime(2000, 1, 2), False, False, False],
            [datetime(2000, 1, 2), datetime(2000, 1, 2, 0, 15), True, True, False],
        ],
    )
    user_id = 69

    output = tested_module._to_dict(
        activity_sessions=activity_sessions, user_id=user_id
    )

    assert output == [
        {
            "user_id": user_id,
            "start_time": datetime(2000, 1, 1),
            "end_time": datetime(2000, 1, 1, 1),
            "is_active": True,
            "is_focus": False,
            "is_break": False,
        },
        {
            "user_id": user_id,
            "start_time": datetime(2000, 1, 1, 1),
            "end_time": datetime(2000, 1, 2),
            "is_active": False,
            "is_focus": False,
            "is_break": False,
        },
        {
            "user_id": user_id,
            "start_time": datetime(2000, 1, 2),
            "end_time": datetime(2000, 1, 2, 0, 15),
            "is_active": True,
            "is_focus": True,
            "is_break": False,
        },
    ]
