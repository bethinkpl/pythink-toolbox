from typing import Dict, Any

from flask import Blueprint, request

from chronos.api.user_learning_time import get_learning_time

users_learning_bp = Blueprint("users_learning", __name__)


# TODO: Try to improve the return type hint after FastAPI is introduced.
@users_learning_bp.route("/learning_time", methods=["POST"])
def get_users_learning_time() -> Dict[Any, Any]:
    """
    API end-point | Provides cumulative learning time for a group of users.
    """
    body = request.get_json()
    learning_times = {
        user["id"]: get_learning_time(user["id"], user["start_date"], user["end_date"])
        for user in body["users"]
    }
    return learning_times
