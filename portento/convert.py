"""Functions to convert a stream graph to and from other formats.
"""
import pandas as pd

import portento
from portento.utils import Link
from typing import Union, List

DEFAULT_COL_NAMES = ["interval", "source", "target"]


def _prepare_data_from_columns(df: pd.DataFrame, cols: Union[str, List[str]], names: List[str] = None):
    if isinstance(cols, str):
        return df[cols]
    else:
        names = names if names else cols
        return list(map(lambda attr_vals: tuple(zip(names, attr_vals)),
                        zip(*(df[attr] for attr in cols))))


def from_pandas_stream(df: pd.DataFrame, interval: str = DEFAULT_COL_NAMES[0],
                       source: Union[str, List[str]] = DEFAULT_COL_NAMES[1],
                       target: Union[str, List[str]] = DEFAULT_COL_NAMES[2]):
    """Convert a pandas dataframe to a stream.

    Parameters
    ----------
    df : pandas DataFrame
        Dataframe storing the stream
    interval : str
     Name of the column that stores the intervals of links
    source : str | List(str)
        Name of the column(s) that stores the sources of links
    target : str | List(str)
        Name of the column(s) that stores the targets of links

    Returns
    -------
    stream : StreamGraph
    """
    if not isinstance(df.dtypes[interval], pd.IntervalDtype):
        raise TypeError(f"The {interval} column must be of type: {str(pd.IntervalDtype)} "
                        f"with the same subtype for all rows")

    # TODO look for more itertools

    target = _prepare_data_from_columns(df, target, source)
    source = _prepare_data_from_columns(df, source)

    stream = portento.Stream(links=list(map(lambda x: Link(*x), zip(df[interval], source, target))))

    return stream


# TODO this with tuples
def to_pandas_stream(stream: portento.Stream, interval: str = DEFAULT_COL_NAMES[0],
                     source: str = DEFAULT_COL_NAMES[1],
                     target: str = DEFAULT_COL_NAMES[2]):
    """Convert a stream to a pandas dataframe.

    Parameters
    ----------
    stream : Stream
        A portento Stream
    interval : str
     Name of the column that stores the intervals of links
    source : str
        Name of the column that stores the sources of links
    target : str
        Name of the column that stores the targets of links

    Returns
    -------
    df : pandas Dataframe
    """
    df = pd.DataFrame([(*link,) for link in stream], columns=[interval, source, target])

    return df
