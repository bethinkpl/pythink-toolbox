from datetime import datetime
from typing import Iterable, Mapping, TypedDict, Union

import pymongo  # type: ignore[import]
import chronos.settings
from pymongo.command_cursor import CommandCursor
from pymongo.cursor import Cursor

DATABASE = pymongo.MongoClient(
    host=chronos.settings.MONGO_HOST,
    port=chronos.settings.MONGO_PORT,
    username=chronos.settings.MONGO_USERNAME,
    password=chronos.settings.MONGO_PASSWORD,
)[chronos.settings.MONGO_DATABASE]


class UserDailyTime(TypedDict):
    user_id: int
    time_ms: int
    date_hour: datetime


def read_daily_learning_time(
    user_id: int, start_date: datetime, end_date: datetime
) -> Iterable[UserDailyTime]:
    """
    Read user learning time from mongodb.
    """
    query_results: Cursor = DATABASE["daily_learning_time_view"].find(
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
    cursor: CommandCursor = DATABASE["daily_learning_time_view"].aggregate(
        pipeline=[
            {
                "$match": {
                    "user_id": user_id,
                    "date_hour": {"$gte": start_date, "$lt": end_date},
                }
            },
            {"$group": {"_id": "null", "time_ms": {"$sum": "$time_ms"}}},
            {"$project": {"time_ms": 1, "_id": 0}},
        ],
    )

    results = list(cursor)

    if len(results) == 0:
        return 0

    return results[0]["time_ms"]


def read_daily_break_time(
    user_id: int, start_date: datetime, end_date: datetime
) -> Iterable[UserDailyTime]:
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
) -> Iterable[UserDailyTime]:
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
) -> Iterable[UserDailyTime]:
    rows = [
        UserDailyTime(
            user_id=row["user_id"],
            time_ms=row["time_ms"],
            date_hour=row["date_hour"],
        )
        for row in query_results
    ]

    return rows
