from datetime import datetime
from typing import List

from fastapi.routing import APIRouter
from fastapi import Query

from chronos.api.storage_operations import UserDailyTime, read_daily_focus_time

user_focus_time_daily_router = APIRouter()


@user_focus_time_daily_router.get("/user_focus_daily_router/{user_id}")
def get_user_focus_time_daily(
    user_id: int,
    start_time: datetime = Query(
        ..., alias="start-time", title="", description=""
    ),  # TODO LACE-469 fill when documenting
    end_time: datetime = Query(
        ..., alias="end-time", title="", description=""
    ),  # TODO LACE-469 fill when documenting
) -> List[UserDailyTime]:
    """
    API end-point | Provides user's daily focus time.
    """

    return read_daily_focus_time(
        user_id=user_id,
        start_time=start_time,
        end_time=end_time,
    )
