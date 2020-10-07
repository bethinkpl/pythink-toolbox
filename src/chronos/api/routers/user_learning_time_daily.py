from typing import List

from dateutil.parser import isoparse
from fastapi.routing import APIRouter

from chronos.api.storage_operations import UserDailyTime, read_daily_learning_time

user_learning_time_daily_router = APIRouter()


@user_learning_time_daily_router.get("/user_learning_time_daily/{user_id}")
def get_user_learning_time_daily(
    user_id: int, start_time: str, end_time: str
) -> List[UserDailyTime]:
    """
    API end-point | Provides user's daily learning time.
    """
    learning_time = read_daily_learning_time(
        user_id=user_id,
        start_time=isoparse(start_time),
        end_time=isoparse(end_time),
    )
    return learning_time
