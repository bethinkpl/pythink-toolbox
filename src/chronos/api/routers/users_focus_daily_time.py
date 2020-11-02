from datetime import datetime
from typing import List

from fastapi.routing import APIRouter
from fastapi.param_functions import Query

from chronos.api.storage_operations import UserDailyTime, read_daily_focus_time

users_focus_daily_time_router = APIRouter()


@users_focus_daily_time_router.get("/users_focus_daily_time/{user_id}")
def get_users_focus_daily_time(
    user_id: int,
    range_start: datetime = Query(
        ..., alias="range-start", title="", description=""
    ),  # TODO LACE-469 fill when documenting
    range_end: datetime = Query(
        ..., alias="range-end", title="", description=""
    ),  # TODO LACE-469 fill when documenting
) -> List[UserDailyTime]:
    """
    API end-point | Provides user's daily focus time.
    """

    return read_daily_focus_time(user_id=user_id, time_range=(range_start, range_end))
