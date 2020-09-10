from typing import Any, Dict, List

from fastapi import APIRouter
from pydantic import BaseModel

from api.user_learning_time import get_learning_time

users_learning_router = APIRouter()


class Item(BaseModel):
    id: int
    start_date: str
    end_date: str


class Users(BaseModel):
    users: List[Item]


# TODO: Try to improve the return type hint after FastAPI is introduced.
@users_learning_router.post("/learning_time")
def get_users_learning_time(item: Users) -> Dict[Any, Any]:
    """
    API end-point | Provides cumulative learning time for a group of users.
    """
    learning_times = {
        user.id: get_learning_time(user.id, user.start_date, user.end_date)
        for user in item.users
    }
    return learning_times
