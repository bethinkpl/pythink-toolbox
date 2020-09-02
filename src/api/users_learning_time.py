from typing import Dict, Tuple

from flask import request

from src.api.api import app
from src.api.helpers import get_learning_time


@app.route('/learning_time', methods=['POST'])
def get_users_learning_time() -> Tuple[Dict[str, dict], int]:
    body = request.get_json()
    learning_times = {
        user["id"]: get_learning_time(user["id"], user["start_date"], user["end_date"])
        for user in body["users"]
    }
    return {'users_learning_time': learning_times}, 200

