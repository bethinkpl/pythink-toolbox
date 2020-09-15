from typing import Dict, Any

from chronos.api.models import User
from chronos.api.user_learning_time_daily import get_learning_time_daily
from fastapi import APIRouter

user_learning_router = APIRouter()


# pylint: disable=unused-argument,fixme
# FIXME Connect to data source https://bethink.atlassian.net/browse/LACE-453
def get_learning_time(user_id: int, start_date: str, end_date: str) -> int:
    """
    Get learning time from the data source.
    """
    return sum(
        get_learning_time_daily(
            user_id=user_id, start_date=start_date, end_date=end_date
        ).values()
    )


# TODO: Try to improve the return type hint after FastAPI is introduced.
@user_learning_router.post("/learning_time/{user_id}")
def get_user_learning_time(user_id: int, user: User) -> Dict[Any, Any]:
    """
    API end-point | Provides user's cumulative learning time.
    """
    user_learning_time = get_learning_time(user_id, user.start_date, user.end_date)
    return {user_id: user_learning_time}
