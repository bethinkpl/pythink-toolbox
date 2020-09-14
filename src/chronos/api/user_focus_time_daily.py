from typing import Dict

from chronos.api.models import Item
from fastapi import APIRouter

focus_daily_router = APIRouter()


# pylint: disable=unused-argument,fixme
# FIXME Connect to data source https://bethink.atlassian.net/browse/LACE-453
def get_focus_time_daily(
    user_id: int, start_date: str, end_date: str
) -> Dict[str, int]:
    """
    Get daily focus time from the data source.
    """
    return {"2000-01-01": 150, "2000-01-02": 20, "2000-01-03": 150}


@focus_daily_router.post("/focus_time_daily/{user_id}")
def get_user_focus_time_daily(user_id: int, item: Item) -> Dict[str, int]:
    """
    API end-point | Provides user's daily focus time.
    """
    return get_focus_time_daily(user_id, item.start_date, item.end_date)
