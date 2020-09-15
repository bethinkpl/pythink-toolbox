from dateutil.parser import isoparse

from chronos.api.models import User
from fastapi import APIRouter

from chronos.api.storage import read_daily_break_time

break_daily_router = APIRouter()


@break_daily_router.post("/break_time_daily/{user_id}")
def get_user_break_time_daily(user_id: int, user: User) -> Dict[str, int]:
    """
    API end-point | Provides user's daily break time.
    """
    break_time: str = read_daily_break_time(
        user_id=user_id,
        start_date=isoparse(item.start_date),
        end_date=isoparse(item.end_date),
    )
    return break_time
