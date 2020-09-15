from typing import Dict, List

from fastapi.routing import APIRouter
from fastapi.param_functions import Body
from dateutil.parser import isoparse

from chronos.api.models import User
from chronos.api.storage import read_cumulative_learning_time

users_learning_router = APIRouter()


@users_learning_router.post("/learning_time")
def get_users_learning_time(
    users: List[User] = Body(..., embed=True)
) -> Dict[int, int]:
    """
    API end-point | Provides cumulative learning time for a group of users.
    """
    learning_times = {
        user.id: read_cumulative_learning_time(
            user_id=user.id,
            start_date=isoparse(user.start_date),
            end_date=isoparse(user.end_date),
        )
        for user in users
    }
    return learning_times
