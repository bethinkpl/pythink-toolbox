from datetime import datetime
from typing import Dict

from dateutil.parser import isoparse
from flask import jsonify, request, Blueprint
from storage import read_daily_break_time

break_daily_bp = Blueprint("break", __name__)


@break_daily_bp.route("/break_time_daily/<int:user_id>", methods=["POST"])
def get_user_break_time_daily(user_id: int) -> Dict[str, int]:
    """
    API end-point | Provides user's daily break time.
    """
    body = request.get_json()
    return get_break_time_daily(
        user_id, isoparse(body["start_date"]), isoparse(body["end_date"])
    )


def get_break_time_daily(
    user_id: int, start_date: datetime, end_date: datetime
) -> Dict[str, int]:
    """
    Get daily break time from the data source.
    """

    break_time = read_daily_break_time(
        user_id=user_id, start_date=start_date, end_date=end_date
    )

    response = jsonify(
        [
            {
                "user_id": row["user_id"],
                "break_time_ms": row["time_ms"],
                "date_hour": row["date_hour"].isoformat(),
            }
            for row in break_time
        ]
    )

    return response
