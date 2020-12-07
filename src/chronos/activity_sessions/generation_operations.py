import logging
from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
import pandera

import chronos
from chronos.storage.schemas import ActivitySessionSchema

MAX_DURATION_BETWEEN_EVENTS_TO_CREATE_SESSION = pd.Timedelta(minutes=5)
MIN_FOCUS_DURATION = pd.Timedelta(minutes=15)
MAX_BREAK_DURATION = pd.Timedelta(minutes=30)

logger = logging.getLogger(__name__)


def generate_user_activity_sessions(
    user_id: int,
    activity_events: pd.Series,
    last_active_session: Optional[Dict[str, datetime]],
) -> List[ActivitySessionSchema]:
    """Perform all operations to create activity_sessions for user."""

    logger.info("Creating activity_sessions for user %i.", user_id)

    activity_sessions: List[ActivitySessionSchema] = (
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
    _log_pandas_object("activity_events", activity_events)

    assert not activity_events.empty

    return (
        activity_events.rename("end_time")
        .to_frame()
        .assign(start_time=lambda df: df.end_time - pd.Timedelta(minutes=1))
        .sort_values("start_time")[["start_time", "end_time"]]
    )


def _add_last_active_session(
    initialized_sessions: pd.DataFrame,
    last_active_session: Optional[Dict[str, datetime]],
) -> pd.DataFrame:

    _log_pandas_object("initialized_sessions", initialized_sessions)

    assert not initialized_sessions.empty

    if last_active_session is not None:
        return pd.concat(
            [
                pd.DataFrame.from_dict(last_active_session, orient="index").T,
                initialized_sessions,
            ],
            ignore_index=True,
        )

    return initialized_sessions


def _create_active_sessions(sessions_with_last_active: pd.DataFrame) -> pd.DataFrame:
    _log_pandas_object("sessions_with_last_active", sessions_with_last_active)

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
    _log_pandas_object("active_sessions", active_sessions)

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
    _log_pandas_object("active_and_inactive_sessions", active_and_inactive_sessions)

    assert not active_and_inactive_sessions.empty

    return active_and_inactive_sessions.assign(
        duration=lambda df: df.end_time.sub(df.start_time),
        is_focus=lambda df: df.is_active & df.duration.ge(MIN_FOCUS_DURATION),
    )


def _determine_if_break(activity_sessions_with_focus: pd.DataFrame) -> pd.DataFrame:
    _log_pandas_object("activity_sessions_with_focus", activity_sessions_with_focus)

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

    logger.debug("activity_sessions validated 😎")

    return activity_sessions


def _to_dict(
    activity_sessions: pd.DataFrame, user_id: int
) -> List[ActivitySessionSchema]:

    assert not activity_sessions.empty

    records = activity_sessions.assign(user_id=user_id)[
        ["user_id", "start_time", "end_time", "is_active", "is_focus", "is_break"]
    ].to_dict(orient="records")

    for record in records:
        record["start_time"] = record["start_time"].to_pydatetime()
        record["end_time"] = record["end_time"].to_pydatetime()
        record["version"] = chronos.__version__

    activity_sessions_records: List[ActivitySessionSchema] = records

    logger.debug("activity_sessions to insert to mongo db: \n %s", activity_sessions)

    return activity_sessions_records


def _log_pandas_object(identifier: str, data: Union[pd.Series, pd.DataFrame]) -> None:
    msg = f"{identifier}: \n %s"
    logger.debug(msg, data.to_string())
