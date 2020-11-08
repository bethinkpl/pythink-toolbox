from datetime import datetime
from typing import TypedDict, Literal


class ActivitySessionSchema(TypedDict):
    """Activity sessions schema."""

    user_id: int
    start_time: datetime
    end_time: datetime
    is_active: bool
    is_focus: bool
    is_break: bool
    version: str


class _MaterializedViewIDSchema(TypedDict):
    user_id: int
    start_time: datetime


class MaterializedViewSchema(TypedDict):
    _id: _MaterializedViewIDSchema
    end_time: datetime
    duration_ms: int


class UsersGenerationStatuesSchema(TypedDict, total=False):
    """Users Generation  Statues schema."""

    user_id: int
    last_status: Literal["succeed", "failed"]
    time_until_generations_successful: datetime
    version: str


class _TimeRangeSchema(TypedDict):
    start: datetime
    end: datetime


class GenerationsSchema(TypedDict, total=False):
    time_range: _TimeRangeSchema
    start_time: datetime
    end_time: datetime
