from datetime import datetime
from typing import Iterable, Mapping, Union

import pymongo  # type: ignore[import]
import chronos.settings
from chronos.api.errors import TransformationError

DATABASE = pymongo.MongoClient(
    host=chronos.settings.MONGO_HOST,
    port=chronos.settings.MONGO_PORT,
    username=chronos.settings.MONGO_USERNAME,
    password=chronos.settings.MONGO_PASSWORD,
)[chronos.settings.MONGO_DATABASE]


def read_daily_learning_time(
    user_id: int, start_date: datetime, end_date: datetime
) -> Iterable[Mapping[str, Union[int, str, datetime]]]:
    """
    Read user learning time from mongodb.
    """
    query_results = DATABASE["daily_learning_time_view"].find(
        filter={
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
    result = DATABASE["daily_learning_time_view"].find(
        filter={
            "user_id": user_id,
            "date_hour": {"$gte": start_date, "$lt": end_date},
        },
        projection={"time_ms": 1, "_id": 0},
    )

    # FIXME Use $sum aggregation after migration from datatosk to pymongo. pylint: disable=fixme
    cumulative_learning_time: int = sum([row["time_ms"] for row in result])

    return cumulative_learning_time


def read_daily_break_time(
    user_id: int, start_date: datetime, end_date: datetime
) -> Iterable[Mapping[str, Union[int, str, datetime]]]:
    """
    Read user focus time from mongodb.
    """
    query_results = DATABASE["daily_break_time_view"].find(
        filter={
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
    query_results = DATABASE["daily_focus_time_view"].find(
        filter={
            "user_id": user_id,
            "date_hour": {"$gte": start_date, "$lt": end_date},
        },
    )

    return _transform_daily_time(query_results=query_results)


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
                "time_ms": row["time_ms"],
                "date_hour": date_hour,
            }
        )

    return rows
