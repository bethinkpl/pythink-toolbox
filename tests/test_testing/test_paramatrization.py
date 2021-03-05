# pylint:disable=missing-class-docstring
# pylint:disable=missing-function-docstring
# pylint:disable=protected-access
# pylint:disable=too-few-public-methods
import typing

import pythink_toolbox.testing.parametrization as tested_module


class ScenarioTest(tested_module.Scenario):
    var: typing.Any


def test__transform_scenarios_for_parametrization() -> None:
    test_scenarios = [ScenarioTest(desc="", var=1)]

    expected_output = ("var", [1])
    actual_output = tested_module._transform_scenarios_for_parametrization(
        test_scenarios
    )
    assert expected_output == actual_output
