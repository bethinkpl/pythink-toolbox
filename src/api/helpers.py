from typing import Dict


def get_learning_time(user_id: int, start_date: str, end_date: str) -> int:
    return sum(get_learning_time_daily(user_id=user_id, start_date=start_date, end_date=end_date).values())


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
