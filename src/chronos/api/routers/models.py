from pydantic import BaseModel


class UserDailyModel(BaseModel):
    """
    Data model returning user time for a single day specified by 'date'.
    - **date**: date in ISO format
    - **duration_ms**: total learning time in milliseconds
    """

    date: str
    duration_ms: int


class ResponseCumulative(BaseModel):
    """
    Data model returning cumulative user learning time.
    - **duration_ms**: total learning time in milliseconds for the specified time range
    """

    duration_ms: int
