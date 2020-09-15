from typing import Callable, Any


def function_to_mock_target(function: Callable[..., Any]) -> str:
    return f"{function.__module__}.{function.__name__}"
