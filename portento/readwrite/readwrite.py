"""Functions to convert a stream graph to and from other formats.
"""

import pandas as pd

import portento
from portento.utils import Link, DiLink, interval_from_string
from typing import Union, List, Type
from collections import defaultdict

DEFAULT_COL_NAMES = ["interval", "source", "target"]


def from_csv_stream(file: str,
                    interval: str = DEFAULT_COL_NAMES[0],
                    source: Union[str, List[str]] = DEFAULT_COL_NAMES[1],
                    target: Union[str, List[str]] = DEFAULT_COL_NAMES[2],
                    instant_duration: Union[int, float] = 1,
                    **kwargs):
    """Convert a malawi file to a stream.

    Parameters
    ----------
    file : str
        malawi file storing the stream
    interval : str
        Name of the column that stores the intervals of links
    source : str | List(str)
        Name of the column(s) that stores the sources of links
    target : str | List(str)
        Name of the column(s) that stores the targets of links
    instant_duration : int | float
        The duration of an instantaneous event

    **kwargs
        arguments for calling pandas.read_csv

    Returns
    -------
    stream : Stream
    """
    return from_pandas_stream(_prepare_csv(file, interval, **kwargs),
                              interval,
                              source,
                              target,
                              instant_duration)


def from_csv_di_stream(file: str,
                       interval: str = DEFAULT_COL_NAMES[0],
                       source: Union[str, List[str]] = DEFAULT_COL_NAMES[1],
                       target: Union[str, List[str]] = DEFAULT_COL_NAMES[2],
                       instant_duration: Union[int, float] = 1,
                       **kwargs):
    """Convert a malawi file to a directed stream.

    Parameters
    ----------
    file : str
        malawi file storing the directed stream
    interval : str
        Name of the column that stores the intervals of links
    source : str | List(str)
        Name of the column(s) that stores the sources of links
    target : str | List(str)
        Name of the column(s) that stores the targets of links
    instant_duration : int | float
        The duration of an instantaneous event


    **kwargs
    arguments for calling pandas.read_csv

    Returns
    -------
    stream : DiStream
    """

    return from_pandas_di_stream(_prepare_csv(file, interval, **kwargs),
                                 interval,
                                 source,
                                 target,
                                 instant_duration)


def to_csv_stream(file: str,
                  stream: Union[portento.Stream, portento.DiStream],
                  interval: Union[str, List[str]] = DEFAULT_COL_NAMES[0],
                  source: Union[str, List[str]] = DEFAULT_COL_NAMES[1],
                  target: Union[str, List[str]] = DEFAULT_COL_NAMES[2],
                  **kwargs):
    """Convert a stream to a malawi.

    Parameters
    ----------
    file: str
        The output file (will be a malawi)
    stream : Stream
        A portento Stream
    interval : str
        Name of the column that stores the intervals of links
    source : str
        Name of the column that stores the sources of links
    target : str
        Name of the column that stores the targets of links


    **kwargs
            arguments for calling pandas.DataFrame.to_csv

    Returns
    -------
    df : pandas Dataframe
    """
    to_pandas_stream(stream, interval, source, target).to_csv(file, **kwargs)


def from_pandas_stream(df: pd.DataFrame,
                       interval: str = DEFAULT_COL_NAMES[0],
                       source: Union[str, List[str]] = DEFAULT_COL_NAMES[1],
                       target: Union[str, List[str]] = DEFAULT_COL_NAMES[2],
                       instant_duration: Union[int, float] = 1):
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
    instant_duration : int | float
        The duration of an instantaneous event

    Returns
    -------
    stream : Stream
    """

    return _from_pandas(stream_type=portento.Stream,
                        link_type=Link,
                        df=df,
                        interval=interval,
                        source=source,
                        target=target,
                        instant_duration=instant_duration)


def from_pandas_di_stream(df: pd.DataFrame,
                          interval: str = DEFAULT_COL_NAMES[0],
                          source: Union[str, List[str]] = DEFAULT_COL_NAMES[1],
                          target: Union[str, List[str]] = DEFAULT_COL_NAMES[2],
                          instant_duration: Union[int, float] = 1):
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
    instant_duration : int | float
        The duration of an instantaneous event

    Returns
    -------
    stream : DiStream
    """

    return _from_pandas(stream_type=portento.DiStream,
                        link_type=DiLink,
                        df=df,
                        interval=interval,
                        source=source,
                        target=target,
                        instant_duration=instant_duration)


def to_pandas_stream(stream: Union[portento.Stream, portento.DiStream],
                     interval: str = DEFAULT_COL_NAMES[0],
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
                 target: Union[str, List[str]],
                 instant_duration: Union[int, float]):
    if not isinstance(df.dtypes[interval], pd.IntervalDtype):
        raise TypeError(f"The {interval} column must be of type: {str(pd.IntervalDtype)} "
                        f"with the same subtype for all rows")

    if any(((stream_type == portento.Stream and link_type == Link),
            (stream_type == portento.DiStream and link_type == DiLink))):

        target = _prepare_data_from_columns(df, target, names=source)
        source = _prepare_data_from_columns(df, source)

        stream = stream_type(links=list(map(lambda x: link_type(*x), zip(df[interval], source, target))),
                             instant_duration=instant_duration)

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


def _prepare_csv(file: str, interval: str, **kwargs):
    df = pd.read_csv(file, **kwargs)
    df[interval] = df[interval].apply(interval_from_string)
    return df
