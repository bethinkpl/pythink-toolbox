from typing import Any, Dict

from dateutil.parser import isoparse
from fastapi.routing import APIRouter

from chronos.api.models import User
from chronos.api.storage import read_cumulative_learning_time

user_learning_router = APIRouter()


@user_learning_router.post("/learning_time/{user_id}")
def get_user_learning_time(user_id: int, user: User) -> Dict[int, int]:
    """
    API end-point | Provides user's cumulative learning time.
    """
    return {
        user_id: read_cumulative_learning_time(
            user_id=user_id,
            start_date=isoparse(user.start_date),
            end_date=isoparse(user.end_date),
        )
    }
