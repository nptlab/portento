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


def create_quantiles_df(df):
    n_nodes, perc_mean_int_len, _ = df.columns.values[0]
    df = df[(n_nodes, perc_mean_int_len)].quantile([0, .25, .5, .75, 1], axis=1)
    """df[n_nodes, perc_mean_int_len, 'max'] = df[(n_nodes, perc_mean_int_len)].max(axis=1)
    df[n_nodes, perc_mean_int_len, 'min'] = df[(n_nodes, perc_mean_int_len)].min(axis=1)
    df[n_nodes, perc_mean_int_len, 'mean'] = df[(n_nodes, perc_mean_int_len)].mean(axis=1)
    df = df.drop(filter(lambda x: isinstance(x[2], int), df.columns), axis=1)  # the original columns"""
    df = df.applymap(lambda x: x * UNIT_MEASURE)

    return df


def nodes_subset_perc(rnd, node_perc, nodes):
    n_nodes_set = int(len(nodes) * (node_perc / 100))
    return nodes_subset_n(rnd, n_nodes_set, nodes)


def nodes_subset_n(rnd, n_nodes_set, nodes):
    return set(rnd.sample(nodes, n_nodes_set))


def sub_interval(rnd, time_perc, time_interval):
    time_val = int(time_perc * time_interval.length / 100)
    start = rnd.choice(range(time_interval.left, time_interval.right - time_val))
    end = start + time_val

    return start, end
