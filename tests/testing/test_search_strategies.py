# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import hypothesis.strategies
import numpy as np  # type: ignore[import]

import src


@hypothesis.given(
    nan_or_float=src.pythink_toolbox.testing.search_strategies.add_nans_strategy(  # pylint: disable=no-value-for-parameter
        hypothesis.strategies.floats(min_value=0.0, max_value=1.0)
    )
)
def test_add_nans_strategy(nan_or_float: float) -> None:
    assert np.isnan(nan_or_float) or (0 <= nan_or_float <= 1)
