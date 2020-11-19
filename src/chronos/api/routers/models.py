# pylint: disable=too-few-public-methods
# pylint: disable=no-name-in-module
# pylint: disable=no-self-argument
from pydantic import BaseModel


class UserDailyModel(BaseModel):
    """
    Data model returning user time for a single day specified by 'date'.
    - **date**: date in ISO format
    - **duration_ms**: total learning time in milliseconds
    """

    date: str
    duration_ms: int
