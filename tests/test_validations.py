# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
from typing import Any, Type, Callable, List, Optional

import pandera  # type: ignore[import]
import pytest

import src.testing.parametrization
import src.validations


class CheckOutputScenario(src.testing.parametrization.Scenario):
    dtype: Type[Any]
    checks: Optional[List[Callable[..., Any]]]
    input_: Any
    raises_error: bool
    error_msg: Optional[str]


SCENARIOS = [
    CheckOutputScenario(
        desc="Correct dtype.",
        dtype=str,
        checks=None,
        input_="",
        raises_error=False,
        error_msg=None,
    ),
    CheckOutputScenario(
        desc="Incorrect dtype.",
        dtype=str,
        checks=None,
        input_=0,
        raises_error=True,
        error_msg="Output of `test_func` is of wrong dtype.",
    ),
    CheckOutputScenario(
        desc="Correct dtype, check fails.",
        dtype=str,
        checks=[lambda x: x == "b"],
        input_="a",
        raises_error=True,
        error_msg="Validation for `test_func` failed for check of index [0].",
    ),
    CheckOutputScenario(
        desc="Correct dtype, check passes.",
        dtype=str,
        checks=[lambda x: x == "a"],
        input_="a",
        raises_error=False,
        error_msg=None,
    ),
    CheckOutputScenario(
        desc="Multiple checks, all passes.",
        dtype=str,
        checks=[lambda x: x == "ab", lambda x: x.startswith("a")],
        input_="ab",
        raises_error=False,
        error_msg=None,
    ),
    CheckOutputScenario(
        desc="Multiple checks, one passes, one fails.",
        dtype=str,
        checks=[lambda x: x == "ab", lambda x: x.startswith("b")],
        input_="ab",
        raises_error=True,
        error_msg="Validation for `test_func` failed for check of index [1].",
    ),
]


@src.testing.parametrization.parametrize(SCENARIOS)  # type: ignore[misc]
def test_check_output(
    dtype: Type[Any],
    checks: Optional[List[Callable[..., Any]]],
    input_: Any,
    raises_error: bool,
    error_msg: Optional[str],
) -> None:
    @src.validations.check_output(dtype=dtype, checks=checks)
    def test_func(input__: Any) -> Any:
        return input__

    if raises_error:
        with pytest.raises(src.validations.ValidationError, match=error_msg):
            test_func(input_)
    else:
        assert test_func(input_) == input_


class IsJSONScenario(src.testing.parametrization.Scenario):
    test_string: str
    expected_output: bool


SCENARIOS = [
    IsJSONScenario(desc="", test_string="{}", expected_output=True),  # type: ignore[list-item]
    IsJSONScenario(desc="", test_string="{asdf}", expected_output=False),  # type: ignore[list-item]
    IsJSONScenario(desc="", test_string='{ "age":100}', expected_output=True),  # type: ignore[list-item]
    IsJSONScenario(desc="", test_string="{'age':100 }", expected_output=False),  # type: ignore[list-item]
    IsJSONScenario(desc="", test_string='{"age":100 }', expected_output=True),  # type: ignore[list-item]
    IsJSONScenario(desc="", test_string='{"age":100 }', expected_output=True),  # type: ignore[list-item]
    IsJSONScenario(desc="", test_string='[{"age":100 }]', expected_output=True),  # type: ignore[list-item]
    IsJSONScenario(  # type: ignore[list-item]
        desc="", test_string='{"foo":[5,6.8],"foo":"bar"}', expected_output=True
    ),
]


@src.testing.parametrization.parametrize(SCENARIOS)  # type: ignore[misc]
def test_is_json(test_string: str, expected_output: bool) -> None:
    assert src.validations.is_json(test_string) == expected_output


def test_create_id_like_validation_column() -> None:
    for allow_duplicates in [True, False]:
        output = src.validations.create_id_like_validation_column(
            name="test", allow_duplicates=allow_duplicates
        )

        expected_output = pandera.Column(
            pandera.Int64,
            checks=pandera.Check.greater_than_or_equal_to(0),
            allow_duplicates=allow_duplicates,
            required=True,
            name="test",
        )

        assert output == expected_output


class CheckScenario(src.testing.parametrization.Scenario):
    check: Callable[..., Any]
    value: Any
    should_pass: bool


CHECK_SCENARIOS = [
    CheckScenario(
        desc="", check=src.validations.Check.greater(0), value=0, should_pass=False
    ),
    CheckScenario(
        desc="", check=src.validations.Check.greater(0), value=1, should_pass=True
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.greater_or_equal(1),
        value=0,
        should_pass=False,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.greater_or_equal(1),
        value=1,
        should_pass=True,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.less(1),
        value=1,
        should_pass=False,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.less(1),
        value=0,
        should_pass=True,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.less_or_equal(1),
        value=2,
        should_pass=False,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.less_or_equal(1),
        value=1,
        should_pass=True,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.equals(1),
        value=2,
        should_pass=False,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.equals(1),
        value=1,
        should_pass=True,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.no_duplicates(),
        value=[1, 1],
        should_pass=False,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.no_duplicates(),
        value=[1, 2],
        should_pass=True,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.contains_type_only(int),
        value=[1, "1"],
        should_pass=False,
    ),
    CheckScenario(
        desc="",
        check=src.validations.Check.contains_type_only(int),
        value=[1, 2],
        should_pass=True,
    ),
]


@src.testing.parametrization.parametrize(CHECK_SCENARIOS)  # type: ignore[misc]
def test_Check(  # pylint: disable=invalid-name
    check: Callable[..., Any], value: Any, should_pass: bool
) -> None:
    assert check(value) is should_pass