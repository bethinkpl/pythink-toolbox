from flask import Flask

from chronos.api import (
    user_break_time_daily,
    user_focus_time_daily,
    user_learning_time,
    user_learning_time_daily,
    users_learning_time,
)

app = Flask(__name__)

app.register_blueprint(user_break_time_daily.break_daily_bp)
app.register_blueprint(user_focus_time_daily.focus_daily_bp)
app.register_blueprint(user_learning_time.user_learning_bp)
app.register_blueprint(user_learning_time_daily.learning_daily_bp)
app.register_blueprint(users_learning_time.users_learning_bp)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port="5000", debug=True)
