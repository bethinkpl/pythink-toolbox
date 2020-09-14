from dateutil.parser import isoparse
from flask import Blueprint, jsonify, request

from fastapi import APIRouter, HTTPException
from chronos.api.storage import read_daily_learning_time

from chronos.api.models import Item

learning_daily_router = APIRouter()


@learning_daily_router.post("/learning_time_daily/{user_id}")
def get_user_learning_time_daily(user_id: int, item: Item) -> Dict[str, int]:
    """
    API end-point | Provides user's daily learning time.
    """
    learning_time: str = jsonify(
        read_daily_learning_time(
            user_id=user_id,
            start_date=isoparse(item.start_date),
            end_date=isoparse(item.end_date),
        )
    )
    return learning_time
