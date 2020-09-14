# FIXME Migrate this module from datatosk to pymongo; blocked by LACE-466. pylint: disable=fixme
import logging as log
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping, Union

import datatosk  # No need to resolve types from datatosk, we'll migrate to pymongo. type: ignore[import]
import dotenv
import pandas as pd  # type: ignore[import]
from chronos.api.errors import TransformationError

ENV_PATH = Path(".") / ".env"
dotenv.load_dotenv(dotenv_path=ENV_PATH)

mongo_source = datatosk.mongodb("chronos")


def read_daily_learning_time(
    user_id: int, start_date: datetime, end_date: datetime
) -> Iterable[Mapping[str, Union[int, str, datetime]]]:
    """
    Read user learning time from mongodb.
    """
    query_results = mongo_source.read.to_list(
        collection="daily_learning_time_view",
        query_filter={
            "user_id": user_id,
            "date_hour": {"$gte": start_date, "$lt": end_date},
        },
    )

    return _transform_daily_time(query_results=query_results)


def read_cumulative_learning_time(
    user_id: int, start_date: datetime, end_date: datetime
) -> int:
    """
    Read users' cumulative learning time from mongodb.
    """
    result = mongo_source.read.to_list(
        collection="daily_learning_time_view",
        query_filter={
            "user_id": user_id,
            "date_hour": {"$gte": start_date, "$lt": end_date},
        },
        projection={"learning_time_ms": 1, "_id": 0},
    )

    # FIXME Use $sum aggregation after migration from datatosk to pymongo. pylint: disable=fixme
    cumulative_learning_time = sum([row["learning_time_ms"] for row in result])

    return cumulative_learning_time


def read_daily_break_time(
    user_id: int, start_date: datetime, end_date: datetime
) -> Iterable[Mapping[str, Union[int, str, datetime]]]:
    """
    Read user focus time from mongodb.
    """
    query_results = mongo_source.read.to_list(
        collection="daily_break_time_view",
        query_filter={
            "user_id": user_id,
            "date_hour": {"$gte": start_date, "$lt": end_date},
        },
    )

    return _transform_daily_time(query_results=query_results)


def read_daily_focus_time(
    user_id: int, start_date: datetime, end_date: datetime
) -> Iterable[Mapping[str, Union[int, str, datetime]]]:
    """
    Read user focus time from mongodb.
    """
    query_results = mongo_source.read.to_list(
        collection="daily_focus_time_view",
        query_filter={
            "user_id": user_id,
            "date_hour": {"$gte": start_date, "$lt": end_date},
        },
    )

    return _transform_daily_time(query_results=query_results)


def write_activity_sessions(activity_sessions: pd.DataFrame) -> None:
    """
    Write activity sessions to mongodb.
    """
    mongo_source.write(collection="activity_sessions", data=activity_sessions)
    log.info("Wrote %i activity sessions to storage.", len(activity_sessions))


def read_activity_sessions_by_user(user_id: int) -> pd.DataFrame:
    """
    Read activity session from mongodb for a defined user.
    """
    return mongo_source.read.to_pandas(
        collection="activity_sessions",
        query_filter={"user_id": user_id},
    )


def _transform_daily_time(
    query_results: Iterable[Mapping[str, Union[int, str, datetime]]],
) -> Iterable[Mapping[str, Union[int, str, datetime]]]:
    rows = []
    for row in query_results:
        if isinstance(row["date_hour"], datetime):
            date_hour = row["date_hour"].isoformat()
        else:
            raise TransformationError(
                f"""
                    date_hour should be an instance of datetime, {type(row['date_hour'])} given.
                """
            )

        rows.append(
            {
                "user_id": row["user_id"],
                "time_ms": row["learning_time_ms"],
                "date_hour": date_hour,
            }
        )

    return rows
