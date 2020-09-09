import itertools
from datetime import datetime
from typing import List, TypedDict, Optional
import logging

import pandas as pd  # type: ignore[import]
import pandera  # type: ignore[import]

import chronos.activity_events

MAX_DURATION_BETWEEN_EVENTS_TO_CREATE_SESSION = pd.Timedelta(minutes=5)
MIN_FOCUS_DURATION = pd.Timedelta(minutes=15)
MAX_BREAK_DURATION = pd.Timedelta(minutes=30)

logger = logging.getLogger("Create Activity Sessions")


class ActivitySession(TypedDict):
    """Activity sessions schema."""

    user_id: int
    start_time: datetime
    end_time: datetime
    is_active: bool
    is_focus: bool
    is_break: bool


def main(start_time: datetime, end_time: datetime) -> List[ActivitySession]:
    """Create activity_sessions for all users
    who had activity_events between given timestamps."""

    users_activity_events = chronos.activity_events.read(
        start_time=start_time, end_time=end_time
    )

    users_activity_events_groups = users_activity_events.groupby("user_id").client_time

    logger.info(
        "Creating activity_sessions for %i users.", len(users_activity_events_groups)
    )

    user_activity_sessions = (
        _create_user_activity_sessions(
            user_id=user_id,
            activity_events=activity_events,
            last_active_session=_read_last_active_session_for_user(user_id),
        )
        for user_id, activity_events in users_activity_events_groups
    )

    return list(itertools.chain.from_iterable(user_activity_sessions))


def _create_user_activity_sessions(
    user_id: int,
    activity_events: pd.Series,
    last_active_session: Optional[pd.DataFrame],
) -> List[ActivitySession]:

    logger.info("Creating activity_sessions for user %i.", user_id)

    activity_sessions: List[ActivitySession] = (
        activity_events.pipe(_initialize_sessions_creation)
        .pipe(_add_last_active_session, last_active_session=last_active_session)
        .pipe(_create_active_sessions)
        .pipe(_fill_with_inactive_sessions)
        .pipe(_determine_if_focus)
        .pipe(_determine_if_break)
        .pipe(_sessions_validation)
        .pipe(_to_dict, user_id=user_id)
    )

    return activity_sessions


def _initialize_sessions_creation(activity_events: pd.Series) -> pd.DataFrame:
    logger.debug(  # pylint: disable=logging-too-many-args
        "activity_events: \n", activity_events.to_string()
    )
    assert not activity_events.empty

    return (
        activity_events.rename("end_time")
        .to_frame()
        .assign(start_time=lambda df: df.end_time - pd.Timedelta(minutes=1))
        .sort_values("start_time")[["start_time", "end_time"]]
    )


def _add_last_active_session(
    initialized_sessions: pd.DataFrame, last_active_session: Optional[pd.DataFrame]
) -> pd.DataFrame:
    logger.debug(  # pylint: disable=logging-too-many-args
        "initialized_sessions: \n", initialized_sessions.to_string()
    )
    logger.debug(  # pylint: disable=logging-too-many-args
        "last_active_session: \n", last_active_session.to_string()
    )

    assert not initialized_sessions.empty

    if last_active_session is not None:
        return pd.concat([last_active_session, initialized_sessions], ignore_index=True)

    return initialized_sessions


def _create_active_sessions(sessions_with_last_active: pd.DataFrame) -> pd.DataFrame:
    logger.debug(  # pylint: disable=logging-too-many-args
        "sessions_with_last_active: \n", sessions_with_last_active.to_string()
    )

    assert not sessions_with_last_active.empty

    return (
        sessions_with_last_active.assign(
            events_group_id=lambda df_: (
                df_.start_time.sub(df_.end_time.shift(1))
                .gt(MAX_DURATION_BETWEEN_EVENTS_TO_CREATE_SESSION)
                .cumsum()
            )
        )
        .groupby("events_group_id")
        .agg({"start_time": "first", "end_time": "last"})
        .reset_index(drop=True)
        .assign(is_active=True)
    )


def _fill_with_inactive_sessions(active_sessions: pd.DataFrame) -> pd.DataFrame:
    logger.debug(  # pylint: disable=logging-too-many-args
        "active_sessions: \n", active_sessions.to_string()
    )

    assert not active_sessions.empty

    if len(active_sessions) == 1:
        return active_sessions

    inactive_sessions = (
        active_sessions.assign(
            start_time=lambda df: df.start_time.shift(-1), is_active=False
        )
        .rename(columns={"start_time": "end_time", "end_time": "start_time"})
        .dropna()[["start_time", "end_time", "is_active"]]
    )

    return pd.concat([active_sessions, inactive_sessions]).sort_values(
        "start_time", ignore_index=True
    )


def _determine_if_focus(active_and_inactive_sessions: pd.DataFrame) -> pd.DataFrame:
    logger.debug(  # pylint: disable=logging-too-many-args
        "active_and_inactive_sessions: \n", active_and_inactive_sessions.to_string()
    )

    assert not active_and_inactive_sessions.empty

    return active_and_inactive_sessions.assign(
        duration=lambda df: df.end_time.sub(df.start_time),
        is_focus=lambda df: df.is_active & df.duration.ge(MIN_FOCUS_DURATION),
    )


def _determine_if_break(activity_sessions_with_focus: pd.DataFrame) -> pd.DataFrame:
    logger.debug(  # pylint: disable=logging-too-many-args
        "activity_sessions_with_focus: \n", activity_sessions_with_focus.to_string()
    )

    assert not activity_sessions_with_focus.empty

    return activity_sessions_with_focus.assign(
        is_break=lambda df: (
            ~df.is_active
            & df.duration.map(lambda duration: duration <= MAX_BREAK_DURATION)
            & df.is_focus.shift(1)
            & df.is_focus.shift(-1)
        )
    ).drop("duration", axis="columns")


def _sessions_validation(activity_sessions: pd.DataFrame) -> pd.DataFrame:

    schema = pandera.DataFrameSchema(
        {
            "start_time": pandera.Column(
                pandera.DateTime,
                checks=pandera.Check(lambda s: all(s == s.sort_values())),
                allow_duplicates=False,
            ),
            "end_time": pandera.Column(
                pandera.DateTime,
                checks=pandera.Check(lambda s: all(s == s.sort_values())),
                allow_duplicates=False,
            ),
            "is_active": pandera.Column(
                pandera.Bool,
                checks=[pandera.Check(lambda s: s.iloc[0] and s.iloc[-1])],
            ),
            "is_focus": pandera.Column(pandera.Bool),
            "is_break": pandera.Column(pandera.Bool),
        },
        checks=[
            pandera.Check(lambda df_: not df_.empty),
            pandera.Check(
                lambda df_: all(
                    df_.query("is_active == False")
                    .end_time.sub(df_.start_time)
                    .dropna()
                    > MAX_DURATION_BETWEEN_EVENTS_TO_CREATE_SESSION
                )
                or (len(df_.query("is_active == False")) == 0)
            ),
            pandera.Check(
                lambda df_: all(
                    df_.query("is_break == True").end_time.sub(df_.start_time).dropna()
                    <= MAX_BREAK_DURATION
                )
                or (len(df_.query("is_break == True")) == 0)
            ),
            pandera.Check(lambda df_: df_.end_time > df_.start_time),
            pandera.Check(
                lambda df_: df_.end_time.eq(
                    df_.start_time.shift(-1), fill_value=df_.end_time.iloc[-1]
                )
            ),
        ],
        index=pandera.Index(
            pandera.Int64,
            checks=pandera.Check.greater_than_or_equal_to(0),
            allow_duplicates=False,
        ),
        strict=True,
    )

    schema.validate(activity_sessions)

    return activity_sessions


def _to_dict(activity_sessions: pd.DataFrame, user_id: int) -> List[ActivitySession]:

    assert not activity_sessions.empty

    records = activity_sessions.assign(user_id=user_id)[
        ["user_id", "start_time", "end_time", "is_active", "is_focus", "is_break"]
    ].to_dict(orient="records")

    for record in records:
        record["start_time"] = record["start_time"].to_pydatetime()
        record["end_time"] = record["end_time"].to_pydatetime()

    activity_sessions_records: List[ActivitySession] = records

    return activity_sessions_records


def _read_last_active_session_for_user(
    user_id: int,  # pylint: disable=unused-argument
) -> Optional[pd.DataFrame]:
    # FIXME move to different place? pylint: disable=fixme
    # FIXME pop last active session from mongo pylint: disable=fixme
    return pd.DataFrame(
        {
            "start_time": [pd.Timestamp("2018-12-14T10:40:19.691Z")],
            "end_time": [pd.Timestamp("2018-12-14T10:45:28.421Z")],
        }
    )
