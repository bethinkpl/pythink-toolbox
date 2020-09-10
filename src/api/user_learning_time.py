from typing import Any, Dict

from dateutil.parser import isoparse
from flask import Blueprint, request
from storage import read_cumulative_learning_time

user_learning_bp = Blueprint("user_learning", __name__)


# TODO: Try to improve the return type hint after FastAPI is introduced.
@user_learning_bp.route("/learning_time/<int:user_id>", methods=["POST"])
def get_user_learning_time(user_id: int) -> Dict[Any, Any]:
    """
    API end-point | Provides user's cumulative learning time.
    """
    body = request.get_json()
    return {
        user_id: read_cumulative_learning_time(
            user_id=user_id,
            start_date=isoparse(body["start_date"]),
            end_date=isoparse(body["end_date"]),
        )
    }
