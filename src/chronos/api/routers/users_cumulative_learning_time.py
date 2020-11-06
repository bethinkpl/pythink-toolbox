from datetime import datetime
from fastapi.param_functions import Query
from fastapi.routing import APIRouter

from chronos.api.routers.description import (
    START_ALIAS,
    START_TITLE,
    START_DESC,
    END_ALIAS,
    END_TITLE,
    END_DESC,
    CLT_TAG,
    CLT_DESC,
)
from chronos.api.storage_operations import read_cumulative_learning_time

users_cumulative_learning_time_router = APIRouter()


@users_cumulative_learning_time_router.get(
    "/users_cumulative_learning_time/{user_id}",
    tags=CLT_TAG,
    response_model=int,
    response_description=CLT_DESC,
)
def get_users_cumulative_learning_time(
    user_id: int,
    range_start: datetime = Query(
        ..., alias=START_ALIAS, title=START_TITLE, description=START_DESC
    ),
    range_end: datetime = Query(
        ..., alias=END_ALIAS, title=END_TITLE, description=END_DESC
    ),
) -> int:
    """
    API end-point | Provides user's cumulative learning time.

    **Description**: get cumulative learning time of a user with ID: int:user_id between 'range_start' and 'range_end'.
    """

    return read_cumulative_learning_time(
        user_id=user_id, time_range=(range_start, range_end)
    )
