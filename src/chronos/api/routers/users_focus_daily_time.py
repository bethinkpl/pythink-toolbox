from datetime import datetime
from typing import List

from fastapi.param_functions import Query
from fastapi.routing import APIRouter

from chronos.api.routers.consts import (
    RANGE_START_ALIAS,
    RANGE_START_TITLE,
    RANGE_START_DESC,
    RANGE_END_ALIAS,
    RANGE_END_TITLE,
    RANGE_END_DESC,
)
from chronos.api.routers.models import UserDailyModel
from chronos.api.storage_operations import UserDailyTime, read_daily_focus_time

users_focus_daily_time_router = APIRouter()


@users_focus_daily_time_router.get(
    "/users_focus_daily_time/{user_id}",
    tags=["Users Focus Time"],
    response_model=List[UserDailyModel],
    response_description="Response delivers a list of time in milliseconds and corresponding date in ISO format.",
)
def get_users_focus_daily_time(
    user_id: int,
    range_start: datetime = Query(
        ...,
        alias=RANGE_START_ALIAS,
        title=RANGE_START_TITLE,
        description=RANGE_START_DESC,
    ),
    range_end: datetime = Query(
        ..., alias=RANGE_END_ALIAS, title=RANGE_END_TITLE, description=RANGE_END_DESC
    ),
) -> List[UserDailyTime]:
    """
    API end-point | Provides user's daily focus time.

    **Description**: get focus time of a user with ID: int:user_id divided into days
    between 'range_start' and 'range_end'.
    """

    return read_daily_focus_time(user_id=user_id, time_range=(range_start, range_end))
