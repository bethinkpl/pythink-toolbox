from datetime import datetime
from typing import TypedDict


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


class UserGenerationFailedSchema(TypedDict):
    user_id: int
    reference_time: datetime
