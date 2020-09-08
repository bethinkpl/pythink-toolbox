from tests.api.consts.api_tests import URL_BASE


def get_url(route: str) -> str:
    """
    Get full URL based on a route.
    """
    return f"{URL_BASE}{route}"
