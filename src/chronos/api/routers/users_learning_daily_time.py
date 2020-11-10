from typing import List
from datetime import datetime

from fastapi.param_functions import Query
from fastapi.routing import APIRouter

from chronos.api.storage_operations import UserDailyTime, read_daily_learning_time

users_learning_daily_time_router = APIRouter()


@users_learning_daily_time_router.get("/users_learning_daily_time/{user_id}")
def get_users_learning_daily_time(
    user_id: int,
    range_start: datetime = Query(
        ..., alias="range-start", title="", description=""
    ),  # TODO LACE-469 fill when documenting
    range_end: datetime = Query(
        ..., alias="range-end", title="", description=""
    ),  # TODO LACE-469 fill when documenting
) -> List[UserDailyTime]:
    """
    API end-point | Provides user's daily learning time.
    """

    return read_daily_learning_time(
        user_id=user_id, time_range=(range_start, range_end)
    )