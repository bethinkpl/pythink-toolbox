from typing import Dict, Iterable

from dateutil.parser import isoparse
from fastapi.routing import APIRouter

from chronos.api.models import User
from chronos.api.storage import UserDailyTime, read_daily_focus_time

focus_daily_router = APIRouter()


@focus_daily_router.post("/focus_time_daily/{user_id}")
def get_user_focus_time_daily(user_id: int, user: User) -> Iterable[UserDailyTime]:
    """
    API end-point | Provides user's daily focus time.
    """
    focus_time = read_daily_focus_time(
        user_id=user_id,
        start_date=isoparse(user.start_date),
        end_date=isoparse(user.end_date),
    )
    return focus_time
