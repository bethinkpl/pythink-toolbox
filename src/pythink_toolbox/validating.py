from typing import Callable, Any, Type, Optional, Sequence
import json

import pandera  # type: ignore[import]


class ValidationError(Exception):
    """Raised when validation fails."""


class Check:
    """Predefined checks for check_output() validation decorator."""

    same = lambda x: x

    @classmethod
    def greater(cls, value: Any, map_: Callable[..., Any] = same) -> Callable[..., Any]:
        """Check if object value is greater than value."""
        return lambda x: map_(x) > value

    @classmethod
    def greater_or_equal(
        cls, value: Any, map_: Callable[..., Any] = same
    ) -> Callable[..., Any]:
        """Check if object value is greater or equal than value."""
        return lambda x: map_(x) >= value

    @classmethod
    def less(cls, value: Any, map_: Callable[..., Any] = same) -> Callable[..., Any]:
        """Check if object value is less than value."""
        return lambda x: map_(x) < value

    @classmethod
    def less_or_equal(
        cls, value: Any, map_: Callable[..., Any] = same
    ) -> Callable[..., Any]:
        """Check if object value is less or equal than value."""
        return lambda x: map_(x) <= value

    @classmethod
    def equals(cls, value: Any, map_: Callable[..., Any] = same) -> Callable[..., Any]:
        """Check if object value equals value."""
        return lambda x: map_(x) == value

    @classmethod
    def no_duplicates(cls, map_: Callable[..., Any] = same) -> Callable[..., Any]:
        """Check if object contains duplicates."""
        return lambda sequence: len(set(map_(sequence))) == len(map_(sequence))

    @classmethod
    def contains_type_only(
        cls, type_: Type[Any], map_: Callable[..., Any] = same
    ) -> Callable[..., Any]:
        """Check if object contains only object of type_."""
        return lambda sequence: all(isinstance(x, type_) for x in map_(sequence))


def check_output(
    dtype: Type[Any], checks: Optional[Sequence[Callable[..., Any]]] = None
) -> Callable[..., Any]:
    """Decorator used for custom checking output of specified dtype.

    Args:
        dtype: type of output
        checks: list of functions too check output

    Raises:
        ValidationError: when validation fails.

    Examples:
        >>> @check_output(int, checks=[lambda x: str(x).isnumeric(), Check.greater(0)])
        >>> def func(some_int: int):
        >>>     return some_int
        >>>
        >>> func(0)
        ValidationError: Validation for `func` failed for check of index 1.
    """

    def decorator_check_output(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:

            func_output = func(*args, **kwargs)

            if not isinstance(func_output, dtype):
                raise ValidationError(f"Output of `{func.__name__}` is of wrong dtype.")

            if checks is not None:
                for i, check in enumerate(checks):
                    if not check(func_output):
                        raise ValidationError(
                            f"Validation for `{func.__name__}` failed for check of index {i}."
                        )
            return func_output

        return wrapper

    return decorator_check_output


def is_json(json_string: str) -> bool:
    """Check if passed string is json."""
    try:
        json.loads(json_string)
    except ValueError:
        return False
    return True


def create_id_like_validation_column(
    name: Optional[str] = None, allow_duplicates: bool = True
) -> pandera.Column:
    """Helper for creating id-like pandera Column."""
    return pandera.Column(
        pandera.Int64,
        checks=pandera.Check.greater_than_or_equal_to(0),
        allow_duplicates=allow_duplicates,
        required=True,
        name=name,
    )
