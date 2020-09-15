from dateutil.parser import isoparse
from flask import Blueprint, jsonify, request

from chronos.api.models import User
from fastapi import APIRouter

from chronos.api.storage import read_daily_focus_time

focus_daily_router = APIRouter()


@focus_daily_router.post("/focus_time_daily/{user_id}")
def get_user_focus_time_daily(user_id: int, user: User) -> Dict[str, int]:
    """
    API end-point | Provides user's daily focus time.
    """
    focus_time = read_daily_focus_time(
        user_id=user_id,
        start_date=isoparse(item.start_date),
        end_date=isoparse(item.end_date),
    )
    return focus_time
