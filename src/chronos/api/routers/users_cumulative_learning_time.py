from datetime import datetime

from fastapi.param_functions import Query
from fastapi.routing import APIRouter

from chronos.api.routers import consts
from chronos.api.storage_operations import read_cumulative_learning_time

users_cumulative_learning_time_router = APIRouter()


@users_cumulative_learning_time_router.get(
    "/users_cumulative_learning_time/{user_id}",
    tags=["Cumulative Learning Time For All Users"],
    response_model=int,
    response_description="Response delivers cumulative time in milliseconds.",
)
def get_users_cumulative_learning_time(
    user_id: int,
    range_start: datetime = Query(
        ...,
        alias=consts.RANGE_START_ALIAS,
        title=consts.RANGE_START_TITLE,
        description=consts.RANGE_START_DESC,
    ),
    range_end: datetime = Query(
        ...,
        alias=consts.RANGE_END_ALIAS,
        title=consts.RANGE_END_TITLE,
        description=consts.RANGE_END_DESC,
    ),
) -> int:
    """
    API end-point | Provides user's cumulative learning time.

    **Description**: get cumulative learning time of a user with ID: int:user_id between 'range_start' and 'range_end'.
    """

    return read_cumulative_learning_time(
        user_id=user_id, time_range=(range_start, range_end)
    )
