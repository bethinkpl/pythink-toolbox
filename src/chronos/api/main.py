import uvicorn
from fastapi.applications import FastAPI

from chronos.api.routers import (
    user_break_time_daily,
    user_focus_time_daily,
    user_learning_time,
    user_learning_time_daily,
    users_learning_time,
)

from chronos.logger import logger

logger.init_for_api(name="chronos_api")
app = FastAPI()

app.include_router(user_break_time_daily.break_daily_router)
app.include_router(user_focus_time_daily.focus_daily_router)
app.include_router(user_learning_time.user_learning_router)
app.include_router(user_learning_time_daily.learning_daily_router)
app.include_router(users_learning_time.users_learning_router)


if __name__ == "__main__":
    uvicorn.run(
        "chronos.api.main:app", host="127.0.0.1", port=5000, debug=True, reload=True
    )
