from datetime import datetime

from fastapi.routing import APIRouter
from fastapi import Query

from chronos.api.storage_operations import read_cumulative_learning_time

user_cumulative_learning_time_router = APIRouter()


@user_cumulative_learning_time_router.get("/user_cumulative_learning_time/{user_id}")
def get_user_cumulative_learning_time(
    user_id: int,
    start_time: datetime = Query(
        ..., alias="start-time", title="", description=""
    ),  # TODO LACE-469 fill when documenting
    end_time: datetime = Query(
        ..., alias="end-time", title="", description=""
    ),  # TODO LACE-469 fill when documenting
) -> int:
    """
    API end-point | Provides user's cumulative learning time.
    """

    return read_cumulative_learning_time(
        user_id=user_id,
        start_time=start_time,
        end_time=end_time,
    )
