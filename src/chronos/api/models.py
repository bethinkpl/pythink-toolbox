from typing import List, Optional

from pydantic.main import BaseModel


class User(BaseModel):
    id: Optional[int] = None
    start_date: str
    end_date: str


class Users(BaseModel):
    users: List[User]
