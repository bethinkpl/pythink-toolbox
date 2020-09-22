from typing import Iterable

from dateutil.parser import isoparse
from fastapi.routing import APIRouter

from chronos.api.models import UserLearningTime
from chronos.api.storage import UserDailyTime, read_daily_break_time

break_daily_router = APIRouter()


@break_daily_router.post("/break_time_daily/{user_id}")
def get_user_break_time_daily(
    user_id: int, user_learning_time: UserLearningTime
) -> Iterable[UserDailyTime]:
    """
    API end-point | Provides user's daily break time.
    """
    break_time = read_daily_break_time(
        user_id=user_id,
        start_date=isoparse(user_learning_time.start_date),
        end_date=isoparse(user_learning_time.end_date),
    )
    return break_time
