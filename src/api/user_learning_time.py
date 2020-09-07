from typing import Dict, Tuple

from flask import request, Blueprint

from api.user_learning_time_daily import get_learning_time_daily

user_learning_bp = Blueprint('user_learning', __name__)


def get_learning_time(user_id: int, start_date: str, end_date: str) -> int:
    return sum(get_learning_time_daily(user_id=user_id, start_date=start_date, end_date=end_date).values())


@user_learning_bp.route('/learning_time/<int:user_id>', methods=['POST'])
def get_user_learning_time(user_id: int) -> Tuple[Dict[int, int], int]:
    body = request.get_json()
    user_learning_time = get_learning_time(user_id, body["start_date"], body["end_date"])
    return {user_id: user_learning_time}, 200
