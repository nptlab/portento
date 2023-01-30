from itertools import repeat
import pandas as pd
from setup import UNIT_MEASURE


def str_nodes_perc(n_nodes, perc_mean_int_len):
    return f"n_{n_nodes}-p_{perc_mean_int_len}"


def descriptive_str(n_nodes, perc_mean_int_len, seed):
    return f"stream-{str_nodes_perc(n_nodes, perc_mean_int_len)}-s_{seed}"


def descriptive_str_from_substr(descriptive_substr):
    return f"stream-{descriptive_substr}"


def create_df(n_nodes, perc_mean_int_len, s, performances=None):
    if not performances:
        performances = []

    return pd.DataFrame(performances, columns=pd.MultiIndex.from_tuples([(n_nodes, perc_mean_int_len, s)],
                                                                        names=['n_nodes', 'length_perc', 's']))


def create_min_max_mean_df(df):
    n_nodes, perc_mean_int_len, _ = df.columns.values[0]
    num_measurements = len(df.axes[1])
    df = df.applymap(lambda x: x * UNIT_MEASURE)
    df[n_nodes, perc_mean_int_len, 'max'] = df[(n_nodes, perc_mean_int_len)].max(axis=1)
    df[n_nodes, perc_mean_int_len, 'min'] = df[(n_nodes, perc_mean_int_len)].min(axis=1)
    df[n_nodes, perc_mean_int_len, 'mean'] = df[(n_nodes, perc_mean_int_len)].mean(axis=1)
    df = df.drop(zip(repeat(n_nodes, num_measurements),
                     repeat(perc_mean_int_len, num_measurements),
                     range(num_measurements)), axis=1)

    return df
