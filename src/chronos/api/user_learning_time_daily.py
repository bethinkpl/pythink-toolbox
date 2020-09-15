from typing import Dict, Iterable

from dateutil.parser import isoparse

from fastapi.routing import APIRouter

from chronos.api.models import User
from chronos.api.storage import UserDailyTime, read_daily_learning_time

learning_daily_router = APIRouter()


@learning_daily_router.post("/learning_time_daily/{user_id}")
def get_user_learning_time_daily(user_id: int, user: User) -> Iterable[UserDailyTime]:
    """
    API end-point | Provides user's daily learning time.
    """
    learning_time = read_daily_learning_time(
        user_id=user_id,
        start_date=isoparse(user.start_date),
        end_date=isoparse(user.end_date),
    )
    return learning_time
