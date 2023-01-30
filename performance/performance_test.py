import random
from os import makedirs, remove
from pickle import dump, load
from timeit import Timer
from itertools import product
from portento import Stream
from generate_link import generate_link
from setup import *

import pandas as pd

if not path.exists(STREAM_PICKLE_PATH):
    makedirs(STREAM_PICKLE_PATH)

if not path.exists(ADD_PERFORMANCE_PATH):
    makedirs(ADD_PERFORMANCE_PATH)

combos = product(range(TEST_REP), N_NODES, PERC_MEAN_INT_LEN)  # seed, n_nodes, perc_mean_int_len


def performance_test(seed, n_nodes, perc_mean_int_len):
    desc_str = descriptive_str(n_nodes, perc_mean_int_len, seed)
    filename_performance = path.join(ADD_PERFORMANCE_PATH, desc_str)

    setup = SETUP

    if not path.exists(filename_performance):
        filename_stream = path.join(STREAM_PICKLE_PATH, desc_str)
        setup = "; ".join([setup, f'stream = load(open("{filename_stream}", "rb"))'])

        performances = []
        rnd = random.Random(seed)
        stream = Stream()

        for i in range(MAX_LINKS):
            link = generate_link(rnd, range(n_nodes), perc_mean_int_len, TIME_BOUND)

            if (i + 1) % MEASUREMENT_MILESTONE == 0:
                dump(stream, open(filename_stream, "wb"))
                setup = "; ".join([setup, f'link = {str(link)}'])
                performances.append(Timer(COMMAND, setup).timeit(CMD_REP) / CMD_REP)
                stream = load(open(filename_stream, "rb"))

            stream.add(link)

        df = pd.DataFrame(performances, columns=[n_nodes, perc_mean_int_len])
        df.columns = pd.MultiIndex.from_tuples(df.columns, names=['n_nodes', 'length_perc'])

        dump(df, open(filename_performance, "wb"))
        return df


df = performance_test(0, 5, 10)
print(df)

"""with Pool() as pool:
    pool.starmap(performance_test, combos)"""
# then combine results
# with re: get only interesting pandas dfs;
# combine them...somehow
