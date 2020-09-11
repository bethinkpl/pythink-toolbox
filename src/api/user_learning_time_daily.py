from datetime import datetime
from typing import Dict

from dateutil.parser import isoparse
from flask import Blueprint, jsonify, request
from storage import read_daily_learning_time

learning_daily_bp = Blueprint("learning", __name__)


@learning_daily_bp.route("/learning_time_daily/<int:user_id>", methods=["POST"])
def get_user_learning_time_daily(user_id: int) -> Dict[str, int]:
    """
    API end-point | Provides user's daily learning time.
    """
    body = request.get_json()
    return get_learning_time_daily(
        user_id,
        isoparse(body["start_date"]),
        isoparse(body["end_date"]),
    )


def get_learning_time_daily(
    user_id: int, start_date: datetime, end_date: datetime
) -> Dict[str, int]:
    """
    Get daily learning time from the data source.
    """
    learning_time = read_daily_learning_time(
        user_id=user_id, start_date=start_date, end_date=end_date
    )

    response = jsonify(
        [
            {
                "user_id": row["user_id"],
                "learning_time_ms": row["learning_time_ms"],
                "date_hour": row["date_hour"].isoformat(),
            }
            for row in learning_time
        ]
    )

    return response
