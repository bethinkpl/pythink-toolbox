from datetime import datetime

from fastapi.routing import APIRouter
from fastapi.param_functions import Query

from chronos.api.storage_operations import read_cumulative_learning_time

users_cumulative_learning_time_router = APIRouter()


@users_cumulative_learning_time_router.get("/users_cumulative_learning_time/{user_id}")
def get_users_cumulative_learning_time(
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
