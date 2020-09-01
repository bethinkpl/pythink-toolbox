import sys
from typing import Any

import hypothesis.strategies
import numpy as np  # type: ignore[import]
import pandas as pd  # type: ignore[import]


@hypothesis.strategies.composite
def add_nans_strategy(
    draw: Any, strategy: hypothesis.strategies.SearchStrategy[Any]
) -> Any:
    """Strategy for getting either value from passed strategy or np.NaN."""

    return draw(
        hypothesis.strategies.one_of(hypothesis.strategies.just(np.NaN), strategy)
    )


IDS_STRATEGY = hypothesis.strategies.integers(min_value=0, max_value=sys.maxsize)

TIMESTAMP_STRATEGY = hypothesis.strategies.datetimes(
    min_value=pd.Timestamp.min, max_value=pd.Timestamp.max
).map(pd.Timestamp)

TIMESTAMP_OR_NAN_STRATEGY = add_nans_strategy(  # pylint: disable=no-value-for-parameter
    strategy=TIMESTAMP_STRATEGY
)
