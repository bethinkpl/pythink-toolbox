from pathlib import Path
from typing import List

import datatosk
import dotenv

ENV_PATH = Path(".") / ".env"
dotenv.load_dotenv(dotenv_path=ENV_PATH)

mongo_source = datatosk.mongodb("chronos")


def read_learning_time(user_ids: List):
    pipeline = [
        {
            "$match": {
                "$or": [{"is_active": {"$eq": True}}, {"is_break": {"$eq": True}}]
            }
        },
        {"$match": {"user_id": {"$in": user_ids}}},
        {
            "$group": {
                "_id": "$user_id",
                "time_ms": {"$sum": {"$subtract": ["$session_end", "$session_start"]}},
            }
        },
        {"$project": {"time_ms": 1, "time_h": {"$divide": ["$time_ms", 3600000]}}},
    ]

    return mongo_source.read.to_list(
        collection="activity_sessions", command="aggregate", pipeline=pipeline
    )


def write_activity_sessions():
    pass


if __name__ == "__main__":
    print(read_learning_time([1]))
