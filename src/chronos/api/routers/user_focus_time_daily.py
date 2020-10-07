from typing import List

from dateutil.parser import isoparse
from fastapi.routing import APIRouter

from chronos.api.storage_operations import UserDailyTime, read_daily_focus_time

user_focus_time_daily_router = APIRouter()


@user_focus_time_daily_router.get("/user_focus_daily_router/{user_id}")
def get_user_focus_time_daily(
    user_id: int, start_time: str, end_time: str
) -> List[UserDailyTime]:
    """
    API end-point | Provides user's daily focus time.
    """
    focus_time = read_daily_focus_time(
        user_id=user_id,
        start_time=isoparse(start_time),
        end_time=isoparse(end_time),
    )
    return focus_time
