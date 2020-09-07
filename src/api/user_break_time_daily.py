from typing import Dict

from flask import request, Blueprint

break_daily_bp = Blueprint("break", __name__)


def get_break_time_daily(
    user_id: int, start_date: str, end_date: str
) -> Dict[str, int]:
    return {"2000-01-01": 200, "2000-01-02": 400, "2000-01-03": 50}


@break_daily_bp.route("/break_time_daily/<int:user_id>", methods=["POST"])
def get_user_break_time_daily(user_id: int) -> Dict[str, int]:
    body = request.get_json()
    return get_break_time_daily(user_id, body["start_date"], body["end_date"])
