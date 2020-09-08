from typing import Dict

from flask import request, Blueprint

break_daily_bp = Blueprint("break", __name__)


# pylint: disable=unused-argument,fixme
# FIXME Connect to data source https://bethink.atlassian.net/browse/LACE-453
def get_break_time_daily(
    user_id: int, start_date: str, end_date: str
) -> Dict[str, int]:
    """
    Get daily break time from the data source.
    """
    return {"2000-01-01": 200, "2000-01-02": 400, "2000-01-03": 50}


@break_daily_bp.route("/break_time_daily/<int:user_id>", methods=["POST"])
def get_user_break_time_daily(user_id: int) -> Dict[str, int]:
    """
    API end-point | Provides user's daily break time.
    """
    body = request.get_json()
    return get_break_time_daily(user_id, body["start_date"], body["end_date"])
