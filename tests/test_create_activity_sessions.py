from datetime import datetime
from typing import Optional

import hypothesis
from hypothesis.extra.pandas import series, indexes, data_frames, column
import pandas as pd
import pytest
from pythink_toolbox.testing import parametrization

import src.create_activity_sessions as tested_module


def test_main():
    # FIXME
    assert False


def test__create_user_activity_sessions():
    # FIXME
    assert False


@hypothesis.given(
    activity_events=series(dtype="datetime64[ns]", index=indexes(dtype=int))
)
def test__initialize_sessions_creation(activity_events: pd.Series) -> None:

    hypothesis.assume(not activity_events.empty)

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
    raise_assertion: bool
    expected_output: pd.DataFrame


TEST_SCENARIOS = [
    AddLastActiveSessionScenario(
        desc="empty initialized_sessions",
        initialized_sessions=pd.DataFrame(),
        last_active_session=None,
        raise_assertion=True,
        expected_output=pd.DataFrame(),
    ),
    AddLastActiveSessionScenario(
        desc="last_active_session=None",
        initialized_sessions=pd.DataFrame(
            columns=["start_time", "end_time"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1)]],
        ),
        last_active_session=None,
        raise_assertion=False,
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
        raise_assertion=False,
        expected_output=pd.DataFrame(
            columns=["start_time", "end_time"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1)],
                [datetime(2000, 2, 1), datetime(2000, 2, 12)],
            ],
        ),
    ),
]


@parametrization.parametrize(TEST_SCENARIOS)
def test__add_last_active_session(
    initialized_sessions: pd.DataFrame,
    last_active_session: Optional[pd.DataFrame],
    raise_assertion: bool,
    expected_output: pd.DataFrame,
):
    if raise_assertion:
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
    raise_assertion: bool
    expected_output: pd.DataFrame


TEST_SCENARIOS = [
    CreateActiveSessionsScenario(
        desc="sessions_with_last_active empty -> raise error",
        sessions_with_last_active=pd.DataFrame(),
        raise_assertion=True,
        expected_output=pd.DataFrame(),  # value not relevant
    ),
    CreateActiveSessionsScenario(
        desc="only one record in sessions_with_last_active"
        " -> return the the same frame with additional is_active column",
        sessions_with_last_active=pd.DataFrame(
            columns=["start_time", "end_time"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1)]],
        ),
        raise_assertion=False,
        expected_output=pd.DataFrame(
            columns=["start_time", "end_time", "is_active"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1), True]],
        ),
    ),
    CreateActiveSessionsScenario(
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
        raise_assertion=False,
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


@parametrization.parametrize(TEST_SCENARIOS)
def test__create_active_sessions(
    sessions_with_last_active: pd.DataFrame,
    raise_assertion: bool,
    expected_output: pd.DataFrame,
) -> None:

    if raise_assertion:
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
    raise_assertion: bool
    expected_output: pd.DataFrame


TEST_SCENARIOS = [
    FillWithInactiveSessionsScenario(
        desc="active_sessions is empty -> raise error",
        active_sessions=pd.DataFrame(),
        raise_assertion=True,
        expected_output=pd.DataFrame(),  # value not relevant
    ),
    FillWithInactiveSessionsScenario(
        desc="only one active session -> return the same",
        active_sessions=pd.DataFrame(
            columns=["start_time", "end_time", "is_active"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1), True]],
        ),
        raise_assertion=False,
        expected_output=pd.DataFrame(
            columns=["start_time", "end_time", "is_active"],
            data=[[datetime(2000, 1, 1), datetime(2000, 1, 1), True]],
        ),
    ),
    FillWithInactiveSessionsScenario(
        desc="three active sessions -> return two inactive sessions in between",
        active_sessions=pd.DataFrame(
            columns=["start_time", "end_time", "is_active"],
            data=[
                [datetime(2000, 1, 1), datetime(2000, 1, 1, 0, 6, 2), True],
                [datetime(2000, 1, 1, 0, 11, 3), datetime(2000, 1, 1, 0, 11, 4), True],
                [datetime(2000, 2, 1, 0, 11, 3), datetime(2000, 2, 1, 0, 11, 4), True],
            ],
        ),
        raise_assertion=False,
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


@parametrization.parametrize(TEST_SCENARIOS)
def test__fill_with_inactive_sessions(
    active_sessions: pd.DataFrame, raise_assertion: bool, expected_output: pd.DataFrame
) -> None:
    if raise_assertion:
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
    hypothesis.assume(not active_and_inactive_sessions.empty)
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


def test__determine_if_break():
    assert False


def test__sessions_validation():
    assert False


def test__to_dict():
    assert False
