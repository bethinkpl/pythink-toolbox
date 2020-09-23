from pydantic import BaseModel  # pylint: disable=no-name-in-module


# pylint: disable=too-few-public-methods
class UserLearningTime(BaseModel):
    """
    Data model for handling user learning time request body.
    """

    user_id: int
    start_time: str
    end_time: str
