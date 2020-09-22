from typing import Dict, List

from fastapi.routing import APIRouter
from fastapi.param_functions import Body
from dateutil.parser import isoparse

from chronos.api.models import UserLearningTime
from chronos.api.storage import read_cumulative_learning_time

users_learning_router = APIRouter()


@users_learning_router.post("/learning_time")
def get_users_learning_time(
    users: List[UserLearningTime] = Body(..., embed=True)
) -> Dict[int, int]:
    """
    API end-point | Provides cumulative learning time for a group of users.
    """
    learning_times = {
        user_learning_time.user_id: read_cumulative_learning_time(
            user_id=user_learning_time.user_id,
            start_date=isoparse(user_learning_time.start_date),
            end_date=isoparse(user_learning_time.end_date),
        )
        for user_learning_time in users
    }
    return learning_times
