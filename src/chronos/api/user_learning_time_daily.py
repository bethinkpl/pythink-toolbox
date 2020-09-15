from typing import Dict

from dateutil.parser import isoparse

from fastapi import APIRouter

from chronos.api.models import User
from chronos.api.storage import read_daily_learning_time

learning_daily_router = APIRouter()


@learning_daily_router.post("/learning_time_daily/{user_id}")
def get_user_learning_time_daily(user_id: int, user: User) -> Dict[str, int]:
    """
    API end-point | Provides user's daily learning time.
    """
    learning_time = read_daily_learning_time(
        user_id=user_id,
        start_date=isoparse(user.start_date),
        end_date=isoparse(user.end_date),
    )
    return learning_time
