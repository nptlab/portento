import pandas as pd


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
