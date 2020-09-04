import itertools
from datetime import datetime
from typing import List, TypedDict, Optional
import logging

import pandas as pd

import chronos.activity_events

MAX_TIME_BETWEEN_EVENTS_FOR_CREATE_SESSION = pd.Timedelta(minutes=5)
MIN_FOCUS_SESSION_TIME = pd.Timedelta(minutes=15)
MAX_BREAK_TIME = pd.Timedelta(minutes=30)

logger = logging.getLogger("Create Activity Sessions")


class ActivitySession(TypedDict):
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
        _create_user_activity_sessions(user_id=user_id, activity_events=activity_events)
        for user_id, activity_events in users_activity_events_groups
    )

    return list(itertools.chain.from_iterable(user_activity_sessions))


def _create_user_activity_sessions(
    user_id: int, activity_events: pd.Series
) -> List[ActivitySession]:

    logger.info("Creating activity_sessions for user %i.", user_id)

    return (
        activity_events.pipe(_initialize_sessions_creation)
        .pipe(
            _add_last_active_session,
            last_active_session=_read_last_active_session_for_user(user_id),
        )
        .pipe(_create_active_sessions,)
        .pipe(_fill_with_inactive_sessions)
        .pipe(_determine_if_focus)
        .pipe(_determine_if_break)
        .pipe(_sessions_validation)
        .pipe(_to_dict, user_id=user_id)
    )


def _initialize_sessions_creation(activity_events: pd.Series) -> pd.DataFrame:
    logger.debug("activity_events: \n", activity_events)
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
    logger.debug("initialized_sessions: \n", initialized_sessions)
    logger.debug("last_active_session: \n", last_active_session)

    assert not initialized_sessions.empty

    if last_active_session is not None:
        return pd.concat([last_active_session, initialized_sessions], ignore_index=True)

    return initialized_sessions


def _create_active_sessions(sessions_with_last_active: pd.DataFrame) -> pd.DataFrame:
    logger.debug("sessions_with_last_active: \n", sessions_with_last_active)

    assert not sessions_with_last_active.empty

    return (
        sessions_with_last_active.assign(
            events_group_id=lambda df_: (
                df_.start_time.sub(df_.end_time.shift(1))
                .gt(MAX_TIME_BETWEEN_EVENTS_FOR_CREATE_SESSION)
                .cumsum()
            )
        )
        .groupby("events_group_id")
        .agg({"start_time": "first", "end_time": "last"})
        .reset_index(drop=True)
        .assign(is_active=True)
    )


def _fill_with_inactive_sessions(active_sessions: pd.DataFrame) -> pd.DataFrame:
    logger.debug("active_sessions: \n", active_sessions)

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
    logger.debug("active_and_inactive_sessions: \n", active_and_inactive_sessions)

    assert not active_and_inactive_sessions.empty

    return active_and_inactive_sessions.assign(
        duration=lambda df: df.end_time.sub(df.start_time),
        is_focus=lambda df: df.is_active & df.duration.ge(MIN_FOCUS_SESSION_TIME),
    )


def _determine_if_break(activity_sessions_with_focus: pd.DataFrame) -> pd.DataFrame:
    logger.debug("activity_sessions_with_focus: \n", activity_sessions_with_focus)

    assert not activity_sessions_with_focus.empty

    return activity_sessions_with_focus.assign(
        is_break=lambda df: (
            ~df.is_active
            & df.duration.map(lambda duration: duration < MAX_BREAK_TIME)
            & df.is_focus.shift(1)
            & df.is_focus.shift(-1)
        )
    ).drop("duration", axis="columns")


def _sessions_validation(df: pd.DataFrame) -> pd.DataFrame:

    # FIXME replace with pandera

    if df.empty:
        raise ValidationError("Activity sessions are empty 👎")

    if not df.columns.to_list() == [
        "start_time",
        "end_time",
        "is_active",
        "is_focus",
        "is_break",
    ]:
        raise ValidationError("Activity sessions has wrong columns 👎")

    if not df.iloc[0].is_active:
        raise ValidationError("Activity sessions' first session is not active 👎")

    if not df.iloc[-1].is_active:
        raise ValidationError("Activity sessions' last session is not active 👎")

    if not df.equals(df.drop_duplicates(subset="session_start")):
        logger.debug("Duplicated rows:\n", df[df.duplicated(keep=False)])
        raise ValidationError("Duplicates appeared in activity sessions 👎")

    if not df.equals(df.sort_values(["start_time", "end_time"])):
        raise ValidationError("Activity sessions are not sorted 👎")

    inactives_that_lasts_too_short = (
        df.query("is_active == False")
        .assign(
            duration=lambda df_: df_.end_time.sub(df_.start_time),
            lasts_less_than_should=lambda df_: df_.duration
            < MAX_TIME_BETWEEN_EVENTS_FOR_CREATE_SESSION,
        )
        .query("lasts_less_than_should == True")
    )
    if not inactives_that_lasts_too_short.empty:
        logger.debug(
            "inactives_that_lasts_too_short:\n", inactives_that_lasts_too_short
        )
        raise ValidationError("Inactive sessions lasts less than should👎")

    logger.debug("Activity sessions validated 😎")

    return df


def _to_dict(activity_sessions: pd.DataFrame, user_id) -> List[ActivitySession]:

    assert not activity_sessions.empty

    return activity_sessions.assign(user_id=user_id).to_dict(orient="records")


def _read_last_active_session_for_user(user_id: int) -> Optional[pd.DataFrame]:
    # FIXME move to different place?
    # FIXME pop last active session from mongo
    return pd.DataFrame(
        {
            "start_time": pd.Timestamp("2018-12-14T10:40:19.691Z"),
            "end_time": pd.Timestamp("2018-12-14T10:45:28.421Z"),
        }
    )


class ValidationError(AssertionError):
    """ """  # FIXME fill & move
