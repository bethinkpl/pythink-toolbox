from typing import Dict

from fastapi import APIRouter, HTTPException

from chronos.api.models import Item

break_daily_router = APIRouter()


# pylint: disable=unused-argument,fixme
# FIXME Connect to data source https://bethink.atlassian.net/browse/LACE-453
def get_break_time_daily(
    user_id: int, start_date: str, end_date: str
) -> Dict[str, int]:
    """
    Get daily break time from the data source.
    """
    return {"2000-01-01": 200, "2000-01-02": 400, "2000-01-03": 50}


@break_daily_router.post("/break_time_daily/{user_id}")
def get_user_break_time_daily(user_id: int, item: Item) -> Dict[str, int]:
    """
    API end-point | Provides user's daily break time.
    """
    if not item:
        raise HTTPException(status_code=404, detail="Request body not found")
    return get_break_time_daily(user_id, item.start_date, item.end_date)
