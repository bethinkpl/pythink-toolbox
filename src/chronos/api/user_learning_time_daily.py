from typing import Dict

from chronos.api.models import Item
from fastapi import APIRouter

learning_daily_router = APIRouter()


# pylint: disable=unused-argument,fixme
# FIXME Connect to data source https://bethink.atlassian.net/browse/LACE-453
def get_learning_time_daily(
    user_id: int, start_date: str, end_date: str
) -> Dict[str, int]:
    """
    Get daily learning time from the data source.
    """
    return {"2000-01-01": 100, "2000-01-02": 300, "2000-01-03": 250}


@learning_daily_router.post("/learning_time_daily/{user_id}")
def get_user_learning_time_daily(user_id: int, item: Item) -> Dict[str, int]:
    """
    API end-point | Provides user's daily learning time.
    """
    return get_learning_time_daily(user_id, item.start_date, item.end_date)
