from pydantic import BaseModel  # pylint: disable=no-name-in-module


# pylint: disable=too-few-public-methods
class User(BaseModel):
    """
    Data model for handling user learning time request body.
    """

    id: int
    start_date: str
    end_date: str
