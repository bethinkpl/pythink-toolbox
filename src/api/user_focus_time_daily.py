from typing import Dict

from flask import request, Blueprint

focus_daily_bp = Blueprint('focus', __name__)


def get_focus_time_daily(user_id: int, start_date: str, end_date: str) -> Dict[str, int]:
    return {
        "2000-01-01": 150,
        "2000-01-02": 20,
        "2000-01-03": 150
    }


@focus_daily_bp.route('/focus_time_daily/<int:user_id>', methods=['POST'])
def get_user_focus_time_daily(user_id: int) -> Dict[str, int]:
    body = request.get_json()
    return get_focus_time_daily(user_id, body["start_date"], body["end_date"])
