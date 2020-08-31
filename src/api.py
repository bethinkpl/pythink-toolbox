from random import randint
from typing import Dict, Tuple

from flask import Flask, request

app = Flask(__name__)


def get_learning_time(user_id: int, start_date: str, end_date: str) -> int:
    return user_id + 5


def get_learning_time_daily(user_id: int, start_date: str, end_date: str) -> Dict[str, int]:
    return {
        "2000-01-01": 100,
        "2000-01-02": 300,
        "2000-01-03": 250
    }


def get_break_time_daily(user_id: int, start_date: str, end_date: str) -> Dict[str, int]:
    return {
        "2000-01-01": 200,
        "2000-01-02": 400,
        "2000-01-03": 50
    }


def get_focus_time_daily(user_id: int, start_date: str, end_date: str) -> Dict[str, int]:
    return {
        "2000-01-01": 150,
        "2000-01-02": 20,
        "2000-01-03": 150
    }


@app.route('/learning_time/<int:user_id>', methods=['POST'])
def get_user_learning_time(user_id: int) -> Tuple[Dict[int, int], int]:
    body = request.get_json()
    user_learning_time = get_learning_time(user_id, body["start_date"], body["end_date"])
    return {user_id: user_learning_time}, 200


@app.route('/learning_time', methods=['POST'])
def get_users_learning_time() -> Tuple[Dict[str, dict], int]:
    body = request.get_json()
    learning_times = {
        user["id"]: get_learning_time(user["id"], user["start_date"], user["end_date"])
        for user in body["users"]
    }
    return {'users_learning_time': learning_times}, 200


@app.route('/learning_time_daily/<int:user_id>', methods=['POST'])
def get_user_learning_time_daily(user_id: int) -> Dict[str, int]:
    body = request.get_json()
    return get_learning_time_daily(user_id, body["start_date"], body["end_date"])


@app.route('/break_time_daily/<int:user_id>', methods=['POST'])
def get_user_break_time_daily(user_id: int) -> Dict[str, int]:
    body = request.get_json()
    return get_break_time_daily(user_id, body["start_date"], body["end_date"])


@app.route('/focus_time_daily/<int:user_id>', methods=['POST'])
def get_user_focus_time_daily(user_id: int) -> Dict[str, int]:
    body = request.get_json()
    return get_focus_time_daily(user_id, body["start_date"], body["end_date"])


app.run(host='127.0.0.1', port='5000')
