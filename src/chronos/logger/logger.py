import logging
import sentry_sdk
from chronos.settings import SENTRY_DSN_API, LOG_LEVEL


def init_for_api() -> None:
    """
    initializes API connection with sentry
    """
    logging.getLogger(name="CHRONOS_API").setLevel(LOG_LEVEL)
    sentry_sdk.init(dsn=SENTRY_DSN_API)  # type: ignore[attr-defined]
