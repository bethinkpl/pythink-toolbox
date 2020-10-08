from typing import List
from datetime import datetime

from fastapi.routing import APIRouter
from fastapi.param_functions import Query

from chronos.api.storage_operations import UserDailyTime, read_daily_break_time

users_break_daily_time_router = APIRouter()


@users_break_daily_time_router.get("/users_break_daily_time/{user_id}")
def get_users_break_daily_time(
    user_id: int,
    start_time: datetime = Query(
        ..., alias="start-time", title="", description=""
    ),  # TODO LACE-469 fill when documenting
    end_time: datetime = Query(
        ..., alias="end-time", title="", description=""
    ),  # TODO LACE-469 fill when documenting
) -> List[UserDailyTime]:
    """
    API end-point | Provides user's daily break time.
    """

    return read_daily_break_time(
        user_id=user_id,
        start_time=start_time,
        end_time=end_time,
    )
