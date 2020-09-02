from consts.api_tests import URL_BASE


def get_url(route: str) -> str:
    return f"{URL_BASE}{route}"
