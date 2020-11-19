from datetime import datetime
from typing import List

from fastapi.param_functions import Query
from fastapi.routing import APIRouter

from chronos.api.routers.description import (
    START_ALIAS,
    START_TITLE,
    START_DESC,
    END_ALIAS,
    END_TITLE,
    END_DESC,
)
from chronos.api.routers.models import UserDailyModel
from chronos.api.storage_operations import UserDailyTime, read_daily_break_time

users_break_daily_time_router = APIRouter()


@users_break_daily_time_router.get(
    "/users_break_daily_time/{user_id}",
    tags=["Users Break Time"],
    response_model=List[UserDailyModel],
    response_description="Response delivers a list of time in milliseconds and corresponding date in ISO format.",
)
def get_users_break_daily_time(
    user_id: int,
    range_start: datetime = Query(
        ..., alias=START_ALIAS, title=START_TITLE, description=START_DESC
    ),
    range_end: datetime = Query(
        ..., alias=END_ALIAS, title=END_TITLE, description=END_DESC
    ),
) -> List[UserDailyTime]:
    """
    API end-point | Provides user's daily break time.

    **Description**: get break time of a user with ID: int:user_id divided into days
    between 'range_start' and 'range_end'.
    """

    return read_daily_break_time(user_id=user_id, time_range=(range_start, range_end))
