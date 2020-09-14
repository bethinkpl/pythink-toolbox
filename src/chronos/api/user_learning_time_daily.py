from dateutil.parser import isoparse
from flask import Blueprint, jsonify, request

from chronos.api.storage import read_daily_learning_time

learning_daily_bp = Blueprint("learning", __name__)


@learning_daily_bp.route("/learning_time_daily/<int:user_id>", methods=["POST"])
def get_user_learning_time_daily(user_id: int) -> str:
    """
    API end-point | Provides user's daily learning time.
    """
    body = request.get_json()
    learning_time: str = jsonify(
        read_daily_learning_time(
            user_id=user_id,
            start_date=isoparse(body["start_date"]),
            end_date=isoparse(body["end_date"]),
        )
    )
    return learning_time
