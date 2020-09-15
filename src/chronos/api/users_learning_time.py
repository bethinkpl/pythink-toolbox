from typing import Dict, Any

from chronos.api.user_learning_time import get_learning_time
from fastapi import APIRouter

from chronos.api.models import Users

users_learning_router = APIRouter()


# TODO: Try to improve the return type hint after FastAPI is introduced.
@users_learning_router.post("/learning_time")
def get_users_learning_time(users: Users) -> Dict[Any, Any]:
    """
    API end-point | Provides cumulative learning time for a group of users.
    """
    learning_times = {
        user.id: get_learning_time(user.id, user.start_date, user.end_date)
        for user in users.users
    }
    return learning_times
