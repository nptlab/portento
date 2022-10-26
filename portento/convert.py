"""Functions to convert a stream graph to and from other formats.
"""
import pandas as pd

import portento
from portento.utils import Link

DEFAULT_COL_NAMES = ["interval", "source", "target"]


def from_pandas_stream(df: pd.DataFrame, interval: str = DEFAULT_COL_NAMES[0],
                            source: str = DEFAULT_COL_NAMES[1], target: str = DEFAULT_COL_NAMES[2]):
    """Convert a pandas dataframe to a stream.

    Parameters
    ----------
    df : pandas DataFrame
        Dataframe storing the stream
    interval : str
     Name of the column that stores the intervals of links
    source : str
        Name of the column that stores the sources of links
    target : str
        Name of the column that stores the targets of links

    Returns
    -------
    stream : StreamGraph
    """
    if not isinstance(df.dtypes[interval], pd.IntervalDtype):
        raise TypeError(f"The {interval} column must be of type: {str(pd.IntervalDtype)} "
                        f"with the same subtype for all rows")

    stream = portento.Stream(links=list(map(lambda x: Link(*x), zip(df[interval], df[source], df[target]))))

    return stream


def to_pandas_stream(stream: portento.Stream, interval: str = DEFAULT_COL_NAMES[0],
                     source: str = DEFAULT_COL_NAMES[1], target: str = DEFAULT_COL_NAMES[2]):
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
