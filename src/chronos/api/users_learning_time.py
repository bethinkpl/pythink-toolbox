from typing import Any, Dict

from src.chronos.api.user_learning_time import get_learning_time
from fastapi import APIRouter, HTTPException
from dateutil.parser import isoparse
from flask import Blueprint, request

from chronos.api.models import Users
from chronos.api.storage import read_cumulative_learning_time

users_learning_router = APIRouter()


# TODO: Try to improve the return type hint after FastAPI is introduced.
@users_learning_router.post("/learning_time")
def get_users_learning_time(item: Users) -> Dict[Any, Any]:
    """
    API end-point | Provides cumulative learning time for a group of users.
    """
    learning_times = {
        user["id"]: read_cumulative_learning_time(
            user_id=user["id"],
            start_date=isoparse(user["start_date"]),
            end_date=isoparse(user["end_date"]),
        )
        for user in item
    }
    return learning_times
