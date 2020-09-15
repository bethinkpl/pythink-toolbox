from typing import Any, Dict, List

from fastapi import APIRouter, Body
from dateutil.parser import isoparse

from chronos.api.models import User
from chronos.api.storage import read_cumulative_learning_time

users_learning_router = APIRouter()


# TODO: Try to improve the return type hint after FastAPI is introduced.
@users_learning_router.post("/learning_time")
def get_users_learning_time(
    users: List[User] = Body(..., embed=True)
) -> Dict[Any, Any]:
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
