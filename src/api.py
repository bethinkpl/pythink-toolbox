from random import randint

from flask import Flask, request

app = Flask(__name__)


def get_learning_time(user_id, start_date, end_date):
    return randint(0, 1000000)


def get_activity_sessions(user_id):
    return [{
        "start_date": 15934993494,
        "end_date": 15934993998,
        "status": "active",
        "is_break": False,
        "is_focus": True,
    }, {
        "start_date": 15934993998,
        "end_date": 15934999233,
        "status": "inactive",
        "is_break": True,
        "is_focus": False,
    }]


def get_learning_time_daily(user_id, start_date, end_date):
    return {
        "2000-01-01": 100,
        "2000-01-02": 300,
        "2000-010-3": 250
    }


def get_break_time_daily(user_id, start_date, end_date):
    return {
        "2000-01-01": 100,
        "2000-01-02": 300,
        "2000-010-3": 250
    }


def get_focus_time_daily(user_id, start_date, end_date):
    return {
        "2000-01-01": 100,
        "2000-01-02": 300,
        "2000-010-3": 250
    }


@app.route('/learning_time/<int:user_id>', methods=['POST'])
def get_user_learning_time(user_id):
    user = request.get_json()
    user_learning_time = get_learning_time(user_id, user["start_date"], user["end_date"])
    return {user_id: user_learning_time}, 200


@app.route('/learning_time', methods=['POST'])
def get_users_learning_time():
    body = request.get_json()
    learning_times = {}
    for user in body["users"]:
        user_learning_time = get_learning_time(user["id"], user["start_date"], user["end_date"])
        learning_times[user["id"]] = user_learning_time
    return {'users_learning_time': learning_times}, 200


@app.route('/activity_sessions/<int:user_id>', methods=['POST'])
def get_user_activity_sessions(user_id):
    user_activity_sessions = get_activity_sessions(user_id)
    return {'users_activity_sessions': user_activity_sessions}, 200


@app.route('/learning_time_daily/<int:user_id>', methods=['POST'])
def get_user_learning_time_daily(user_id):
    body = request.get_json()
    return get_learning_time_daily(user_id, body["start_date"], body["end_date"])


@app.route('/break_time_daily/<int:user_id>', methods=['POST'])
def get_user_break_time_daily(user_id):
    body = request.get_json()
    return get_break_time_daily(user_id, body["start_date"], body["end_date"])


@app.route('/focus_time_daily/<int:user_id>', methods=['POST'])
def get_user_focus_time_daily(user_id):
    body = request.get_json()
    return get_focus_time_daily(user_id, body["start_date"], body["end_date"])


app.run()
