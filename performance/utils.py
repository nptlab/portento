import pandas as pd
from setup import *


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
                                                                        names=['n_nodes', 'length_perc', 'res']))


def create_min_max_mean_df(df):
    n_nodes, perc_mean_int_len, _ = df.columns.values[0]
    df[n_nodes, perc_mean_int_len, 'max'] = df[(n_nodes, perc_mean_int_len)].max(axis=1)
    df[n_nodes, perc_mean_int_len, 'min'] = df[(n_nodes, perc_mean_int_len)].min(axis=1)
    df[n_nodes, perc_mean_int_len, 'mean'] = df[(n_nodes, perc_mean_int_len)].mean(axis=1)
    df = df.drop(filter(lambda x: isinstance(x[2], int), df.columns), axis=1)  # the original columns
    df = df.applymap(lambda x: x * UNIT_MEASURE)

    return df


def extract_perc_interval():
    pass
