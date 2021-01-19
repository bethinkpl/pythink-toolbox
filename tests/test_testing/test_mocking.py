# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
from typing import Any, Callable

import pytest

import pythink_toolbox.testing.mocking
from pythink_toolbox.testing.parametrization import parametrize
import pythink_toolbox.validating


@pytest.mark.parametrize(
    "function, expected_output",
    [
        (parametrize, "pythink_toolbox.testing.parametrization.parametrize"),
        (
            pythink_toolbox.validating.check_output,
            "pythink_toolbox.validating.check_output",
        ),
    ],
)
def test_function_to_mock_target(
    function: Callable[..., Any], expected_output: str
) -> None:
    output = pythink_toolbox.testing.mocking.transform_function_to_target_string(
        function
    )
    assert expected_output == output
