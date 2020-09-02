from typing import Dict

from flask import request

from src.api.api import app


def get_learning_time_daily(user_id: int, start_date: str, end_date: str) -> Dict[str, int]:
    return {
        "2000-01-01": 100,
        "2000-01-02": 300,
        "2000-01-03": 250
    }


@app.route('/learning_time_daily/<int:user_id>', methods=['POST'])
def get_user_learning_time_daily(user_id: int) -> Dict[str, int]:
    body = request.get_json()
    return get_learning_time_daily(user_id, body["start_date"], body["end_date"])
