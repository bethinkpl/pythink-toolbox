from typing import Any, Dict, Tuple

from flask import Blueprint, request

from src.api.user_learning_time_daily import get_learning_time_daily

user_learning_bp = Blueprint("user_learning", __name__)


# pylint: disable=unused-argument,fixme
# FIXME Connect to data source https://bethink.atlassian.net/browse/LACE-453
def get_learning_time(user_id: int, start_date: str, end_date: str) -> int:
    """
    Get learning time from the data source.
    """
    return sum(
        get_learning_time_daily(
            user_id=user_id, start_date=start_date, end_date=end_date
        ).values()
    )


@user_learning_bp.route("/learning_time/<int:user_id>", methods=["POST"])
def get_user_learning_time(user_id: int) -> Tuple[Dict[Any, Any], int]:
    """
    API end-point | Provides user's cumulative learning time.
    """
    body = request.get_json()
    user_learning_time = get_learning_time(
        user_id, body["start_date"], body["end_date"]
    )
    return {user_id: user_learning_time}, 200
