from typing import Tuple

from flask import request, Blueprint

from api.user_learning_time import get_learning_time

users_learning_bp = Blueprint('users_learning', __name__)


@users_learning_bp.route('/learning_time', methods=['POST'])
def get_users_learning_time() -> Tuple[dict, int]:
    body = request.get_json()
    learning_times = {
        user["id"]: get_learning_time(user["id"], user["start_date"], user["end_date"])
        for user in body["users"]
    }
    return learning_times, 200

