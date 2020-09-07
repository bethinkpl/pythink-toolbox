from typing import Sequence, Mapping, Tuple, List, Any, TypedDict

import pytest


class Scenario(TypedDict):
    """Default scenario for parametrization.
    Examples:
    >>> class ScenarioFoo(Scenario):
    ...     field: str
    """

    desc: str


def _transform_scenarios_for_parametrization(
    scenarios: Sequence[Mapping[str, object]],
) -> Tuple[str, List[Tuple[object, ...]]]:
    """Helper function for `parametrize()`"""

    argnames = ",".join([arg for arg in scenarios[0].keys() if arg != "desc"])

    argvalues = [
        tuple(scenario[parameter] for parameter in scenario if parameter != "desc")
        for scenario in scenarios
    ]

    return argnames, argvalues


def parametrize(scenarios: Sequence[Mapping[str, object]]) -> Any:
    """Used for parametrization of tests with use of Scenario-based parameters.
    Examples:
    >>> @parametrize(
    ...     [
    ...         ScenarioFoo(desc="foo", field="bar"),
    ...         ScenarioFoo(desc="foo1", field="bar1"),
    ...     ]
    ... )
    ... def test_foo(filed: str) -> None:
    ...     assert field.startswith("bar")
    """

    return pytest.mark.parametrize(  # pylint: disable=not-callable
        *_transform_scenarios_for_parametrization(scenarios)
    )
