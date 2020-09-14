from chronos.api.storage import read_daily_break_time
from dateutil.parser import isoparse
from flask import Blueprint, jsonify, request

break_daily_bp = Blueprint("break", __name__)


@break_daily_bp.route("/break_time_daily/<int:user_id>", methods=["POST"])
def get_user_break_time_daily(user_id: int) -> str:
    """
    API end-point | Provides user's daily break time.
    """
    body = request.get_json()
    break_time: str = jsonify(
        read_daily_break_time(
            user_id=user_id,
            start_date=isoparse(body["start_date"]),
            end_date=isoparse(body["end_date"]),
        )
    )
    return break_time
