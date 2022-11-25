"""Functions to convert a stream graph to and from other formats.
"""
from functools import partial
import pandas as pd

import portento
from portento.utils import Link, DiLink
from typing import Union, List, Type
from collections import defaultdict

DEFAULT_COL_NAMES = ["interval", "source", "target"]


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
    stream : Stream
    """

    return _from_pandas(stream_type=portento.Stream,
                        link_type=Link,
                        df=df,
                        interval=interval,
                        source=source,
                        target=target)


def from_pandas_distream(df: pd.DataFrame, interval: str = DEFAULT_COL_NAMES[0],
                         source: Union[str, List[str]] = DEFAULT_COL_NAMES[1],
                         target: Union[str, List[str]] = DEFAULT_COL_NAMES[2]):
    """Convert a pandas dataframe to a directed stream.

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
    stream : DiStream
    """

    return _from_pandas(stream_type=portento.DiStream,
                        link_type=DiLink,
                        df=df,
                        interval=interval,
                        source=source,
                        target=target)


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
    d = defaultdict(lambda: [])
    for link in stream:
        time, u, v = {interval: link.interval}, \
                     _prepare_dict_from_node(link.u, source), \
                     _prepare_dict_from_node(link.v, target)
        link_dict = {**time, **u, **v}
        for k, v in link_dict.items():
            d[k].append(v)

    df = pd.DataFrame.from_dict(dict(d))

    return df


def _from_pandas(df: pd.DataFrame,
                 stream_type: Union[Type[portento.Stream], Type[portento.DiStream]],
                 link_type: Union[Type[Link], Type[DiLink]],
                 interval: str,
                 source: Union[str, List[str]],
                 target: Union[str, List[str]]):
    if not isinstance(df.dtypes[interval], pd.IntervalDtype):
        raise TypeError(f"The {interval} column must be of type: {str(pd.IntervalDtype)} "
                        f"with the same subtype for all rows")

    if any(((stream_type == portento.Stream and link_type == Link),
            (stream_type == portento.DiStream and link_type == DiLink))):

        # TODO look for more itertools

        target = _prepare_data_from_columns(df, target, names=source)
        source = _prepare_data_from_columns(df, source)

        stream = stream_type(links=list(map(lambda x: link_type(*x), zip(df[interval], source, target))))

        return stream

    else:
        raise AttributeError("A Stream object must accept Link objects only;\n"
                             "A DiStream object must accept DiLink objects only.")


def _prepare_dict_from_node(node, col_name: str):
    if isinstance(node, tuple):
        return dict(tuple(map(lambda x: (col_name + "_" + x[0], x[1]), node)))
    return {col_name: node}


def _prepare_data_from_columns(df: pd.DataFrame, cols: Union[str, List[str]], names: List[str] = None):
    if isinstance(cols, str):
        return df[cols]
    else:
        names = names if names else cols
        return list(map(lambda attr_vals: tuple(zip(names, attr_vals)),
                        zip(*(df[attr] for attr in cols))))
