from typing import Sequence, Mapping, Tuple, Any, TypedDict, Union, Iterable, List

import _pytest.mark
import pytest


# pylint:disable=too-few-public-methods
class Scenario(TypedDict):
    """Default scenario for parametrization.
    Examples:
    >>> class ScenarioFoo(Scenario):
    ...     field: str
    """

    desc: str


def _transform_scenarios_for_parametrization(
    scenarios: Sequence[Mapping[str, Any]],
) -> Tuple[str, Iterable[Union[object, Sequence[object]]]]:
    """Helper function for `parametrize()`"""

    argnames = ",".join([arg for arg in scenarios[0].keys() if arg != "desc"])

    argvalues: List[Union[object, Sequence[object]]] = []
    for scenario in scenarios:
        scenario = dict(scenario)
        scenario.pop("desc")
        if len(scenario) == 1:
            argvalues.append(*scenario.values())
        else:
            argvalues.append(tuple(scenario.values()))

    return argnames, argvalues


def parametrize(
    scenarios: Sequence[Mapping[str, object]]
) -> _pytest.mark.MarkDecorator:
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

    argnames, argvalues = _transform_scenarios_for_parametrization(scenarios)
    return pytest.mark.parametrize(argnames, argvalues)
