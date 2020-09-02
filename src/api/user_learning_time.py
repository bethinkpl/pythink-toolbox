from typing import Dict, Tuple

from flask import request

from src.api.api import app
from src.api.user_learning_time_daily import get_learning_time_daily


def get_learning_time(user_id: int, start_date: str, end_date: str) -> int:
    return sum(get_learning_time_daily(user_id=user_id, start_date=start_date, end_date=end_date).values())


@app.route('/learning_time/<int:user_id>', methods=['POST'])
def get_user_learning_time(user_id: int) -> Tuple[Dict[int, int], int]:
    body = request.get_json()
    user_learning_time = get_learning_time(user_id, body["start_date"], body["end_date"])
    return {user_id: user_learning_time}, 200
