import json
import pathlib
from datetime import datetime

import pymongo.errors

import chronos
from chronos.storage import storage


def main():

    file_path = pathlib.Path(chronos.__path__[0]) / "storage" / "schemaValidation.json"
    print(file_path)

    with open(file_path, "r") as file:
        validator = json.loads(file.read())

    client = storage.mongodb.client

    storage.mongodb.database.create_collection(
        storage.ACTIVITY_SESSIONS_COLLECTION_NAME, validator=validator
    )

    try:
        _test_main()
    except pymongo.errors.PyMongoError as err:
        storage.mongodb.activity_sessions_collection.delete_many({})
        raise RuntimeError("Script doesn't work.") from err


def _test_main():
    try:
        storage.mongodb.activity_sessions_collection.insert_one({"cyk": "pyk"})
    except pymongo.errors.WriteError as err:
        assert err.code == 121

    storage.mongodb.activity_sessions_collection.insert_one(
        {
            "user_id": 0,
            "start_time": datetime(2000, 1, 1),
            "end_time": datetime(2000, 1, 1),
            "is_active": True,
            "is_focus": True,
            "is_break": False,
        }
    )
    storage.mongodb.activity_sessions_collection.find_one_and_delete({})


if __name__ == "__main__":
    main()
