from typing import Callable, Any


def transform_function_to_target_string(function: Callable[..., Any]) -> str:
    """Transform function object to target string,
    which can be used for mocks."""

    return f"{function.__module__}.{function.__name__}"
