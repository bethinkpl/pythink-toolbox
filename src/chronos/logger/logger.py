import logging
import sentry_sdk


def init_for_api(name: str):
    logging.getLogger().setLevel(logging.INFO)
    sentry_sdk.init(
        dsn="https://e17f216a2a59481a8129a279d1b10732@o447783.ingest.sentry.io/5428108"
    )
