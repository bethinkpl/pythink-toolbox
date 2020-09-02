from typing import Dict

from flask import request

from src.api.api import app
from src.api.helpers import get_focus_time_daily


@app.route('/focus_time_daily/<int:user_id>', methods=['POST'])
def get_user_focus_time_daily(user_id: int) -> Dict[str, int]:
    body = request.get_json()
    return get_focus_time_daily(user_id, body["start_date"], body["end_date"])
