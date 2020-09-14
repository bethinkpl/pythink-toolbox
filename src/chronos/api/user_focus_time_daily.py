from chronos.api.storage import read_daily_focus_time
from dateutil.parser import isoparse
from flask import Blueprint, jsonify, request

focus_daily_bp = Blueprint("focus", __name__)


@focus_daily_bp.route("/focus_time_daily/<int:user_id>", methods=["POST"])
def get_user_focus_time_daily(user_id: int) -> str:
    """
    API end-point | Provides user's daily focus time.
    """
    body = request.get_json()
    focus_time: str = jsonify(
        read_daily_focus_time(
            user_id=user_id,
            start_date=isoparse(body["start_date"]),
            end_date=isoparse(body["end_date"]),
        )
    )
    return focus_time
