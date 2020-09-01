# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
from typing import Union

import hypothesis.strategies
import numpy as np  # type: ignore[import]

import src.testing.search_strategies


@hypothesis.given(
    nan_or_float=src.testing.search_strategies.add_nans_strategy(  # pylint: disable=no-value-for-parameter
        hypothesis.strategies.floats(min_value=0.0, max_value=1.0)
    )
)
def test_add_nans_strategy(nan_or_float: Union[np.nan, float]) -> None:
    assert np.isnan(nan_or_float) or (0 <= nan_or_float <= 1)
