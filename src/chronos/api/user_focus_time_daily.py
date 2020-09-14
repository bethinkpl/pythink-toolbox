from datetime import datetime
from typing import Dict

from dateutil.parser import isoparse
from flask import Blueprint, jsonify, request
from storage import read_daily_focus_time

focus_daily_bp = Blueprint("focus", __name__)


@focus_daily_bp.route("/focus_time_daily/<int:user_id>", methods=["POST"])
def get_user_focus_time_daily(user_id: int) -> Dict[str, int]:
    """
    API end-point | Provides user's daily focus time.
    """
    body = request.get_json()
    return get_focus_time_daily(
        user_id, isoparse(body["start_date"]), isoparse(body["end_date"])
    )


def get_focus_time_daily(
    user_id: int, start_date: datetime, end_date: datetime
) -> Dict[str, int]:
    """
    Get daily focus time from the data source.
    """
    focus_time = read_daily_focus_time(
        user_id=user_id, start_date=start_date, end_date=end_date
    )

    response = jsonify(
        [
            {
                "user_id": row["user_id"],
                "focus_time_ms": row["time_ms"],
                "date_hour": row["date_hour"].isoformat(),
            }
            for row in focus_time
        ]
    )

    return response
