from typing import List

from dateutil.parser import isoparse
from fastapi.routing import APIRouter

from chronos.api.models import UserLearningTime
from chronos.api.storage_operations import UserDailyTime, read_daily_learning_time

learning_daily_router = APIRouter()


@learning_daily_router.post("/learning_time_daily/{user_id}")
def get_user_learning_time_daily(
    user_id: int, user_learning_time: UserLearningTime
) -> List[UserDailyTime]:
    """
    API end-point | Provides user's daily learning time.
    """
    learning_time = read_daily_learning_time(
        user_id=user_id,
        start_time=isoparse(user_learning_time.start_time),
        end_time=isoparse(user_learning_time.end_time),
    )
    return learning_time
