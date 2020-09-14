from typing import Any, Dict

from chronos.api.storage import read_cumulative_learning_time
from dateutil.parser import isoparse
from flask import Blueprint, request

users_learning_bp = Blueprint("users_learning", __name__)


# TODO: Try to improve the return type hint after FastAPI is introduced.
@users_learning_bp.route("/learning_time", methods=["POST"])
def get_users_learning_time() -> Dict[Any, Any]:
    """
    API end-point | Provides cumulative learning time for a group of users.
    """
    body = request.get_json()
    learning_times = {
        user["id"]: read_cumulative_learning_time(
            user_id=user["id"],
            start_date=isoparse(user["start_date"]),
            end_date=isoparse(user["end_date"]),
        )
        for user in body["users"]
    }
    return learning_times
