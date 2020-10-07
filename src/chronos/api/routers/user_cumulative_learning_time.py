from typing import Dict

from dateutil.parser import isoparse
from fastapi.routing import APIRouter

from chronos.api.storage_operations import read_cumulative_learning_time

user_cumulative_learning_time_router = APIRouter()


@user_cumulative_learning_time_router.get("/user_cumulative_learning_time/{user_id}")
def get_user_cumulative_learning_time(
    user_id: int, start_time: str, end_time: str
) -> Dict[int, int]:
    """
    API end-point | Provides user's cumulative learning time.
    """
    return {
        user_id: read_cumulative_learning_time(
            user_id=user_id,
            start_time=isoparse(start_time),
            end_time=isoparse(end_time),
        )
    }
