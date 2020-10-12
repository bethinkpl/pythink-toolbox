from datetime import datetime
from typing import List, TypedDict, Any, Dict, Tuple

from chronos.storage.specs import mongodb


class UserDailyTime(TypedDict):
    """
    Data model for daily time records.
    """

    date: str
    duration_ms: int


def read_daily_learning_time(
    user_id: int, time_range: Tuple[datetime, datetime]
) -> List[UserDailyTime]:
    """
    Read daily user learning time from storage.
    """
    mongodb.init_client()
    query_results = (
        mongodb.materialized_views.learning_time_sessions_duration.aggregate(
            pipeline=_get_daily_time_pipeline_query(
                user_id=user_id, time_range=time_range
            )
        )
    )

    return list(query_results)


def read_cumulative_learning_time(
    user_id: int, time_range: Tuple[datetime, datetime]
) -> int:
    """
    Read users' cumulative learning time from storage.
    """
    daily_learning_time = read_daily_learning_time(
        user_id=user_id, time_range=time_range
    )

    return sum(doc["duration_ms"] for doc in daily_learning_time)


def read_daily_break_time(
    user_id: int, time_range: Tuple[datetime, datetime]
) -> List[UserDailyTime]:
    """
    Read daily user break time from storage.
    """
    mongodb.init_client()
    query_results = mongodb.materialized_views.break_sessions_duration.aggregate(
        pipeline=_get_daily_time_pipeline_query(user_id=user_id, time_range=time_range)
    )

    return list(query_results)


def read_daily_focus_time(
    user_id: int, time_range: Tuple[datetime, datetime]
) -> List[UserDailyTime]:
    """
    Read daily user focus time from storage.
    """
    mongodb.init_client()
    query_results = mongodb.materialized_views.focus_sessions_duration.aggregate(
        pipeline=_get_daily_time_pipeline_query(user_id=user_id, time_range=time_range)
    )

    return list(query_results)


def _get_daily_time_pipeline_query(
    user_id: int, time_range: Tuple[datetime, datetime]
) -> List[Dict[str, Any]]:
    return [
        {
            "$match": {
                "_id.user_id": user_id,
                "end_time": {"$gt": time_range[0]},
                "_id.start_time": {"$lt": time_range[1]},
            }
        },
        {
            "$group": {
                "_id": {
                    "user_id": "$_id.user_id",
                    "date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$_id.start_time",
                        }
                    },
                },
                "duration_ms": {"$sum": "$duration_ms"},
            }
        },
        {"$project": {"_id": 0, "date": "$_id.date", "duration_ms": 1}},
        {"$sort": {"date": 1}},
    ]
