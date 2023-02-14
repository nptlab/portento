import random
from multiprocessing import Pool
from os import makedirs, listdir, remove
from pickle import load, dump
from itertools import product
from timeit import Timer

import pandas

from utils import *
from setup import *


def performance_slice(seed, n_nodes, perc_mean_int_len):
    desc_str = descriptive_str(n_nodes, perc_mean_int_len, seed)
    filename_performance = path.join(SLICE_PERFORMANCE_PATH, f"stream-s_{seed}")

    setup = SETUP_SLICE

    if not path.exists(filename_performance):
        filename_stream = path.join(STREAM_PICKLE_PATH, desc_str)
        setup = "; ".join([setup, f'stream = load(open("{filename_stream}", "rb"))'])

        rnd = random.Random(seed)
        nodes = list(load(open(filename_stream, 'rb')).nodes)

        dfs = []

        for perc_nodes, perc_time in product(PERC_NODES, PERC_INTERVAL):
            for precedence in SLICE_TYPES:
                performances = []
                for _ in range(TEST_REP_SLICE):
                    start, end = sub_interval(rnd, perc_time, TIME_BOUND)
                    nodes_for_slice = nodes_subset_perc(rnd, perc_nodes, nodes)

                    perf_setup = "; ".join([setup,
                                            f'slice_type="{precedence}"',
                                            f'time_filter=TimeFilter([Interval({start}, {end}, "both")])',
                                            f'node_filter=NodeFilter(lambda x: x in {nodes_for_slice})'])

                    performances.append(Timer(SLICE_COMMAND, perf_setup).timeit(CMD_REP) / CMD_REP)

                df = create_df(perc_nodes, perc_time, precedence, performances)
                dfs.append(df)

        df = pd.concat(dfs, axis=1)
        dump(df, open(filename_performance, 'wb'))


if __name__ == "__main__":
    if not path.exists(STREAM_PICKLE_PATH):
        raise Exception("Error: create streams before. Run add_performance.main")

    if not path.exists(SLICE_PERFORMANCE_PATH):
        makedirs(SLICE_PERFORMANCE_PATH)

    n_run = range(TEST_REP * len(N_NODES) * len(PERC_MEAN_INT_LEN))  # a different seed for each combination
    combos = product(range(TEST_REP), N_NODES, PERC_MEAN_INT_LEN)  # n, n_nodes, perc_mean_int_len
    combos = [(n_run[i], n_nodes, perc) for i, (_, n_nodes, perc) in enumerate(combos)]  # seed, n_nodes,
    # perc_mean_int_len
    with Pool() as pool:
        pool.starmap(performance_slice, combos)

    """used = []
    for eval_file in listdir(SLICE_PERFORMANCE_PATH):
        filepath = path.join(SLICE_PERFORMANCE_PATH, eval_file)
        df = pd.concat([df, load(open(filepath, 'rb'))], axis=1)
        used.append(filepath)
    dump(df, open(filepath, 'wb'))
    for eval_file in used:
        remove(eval_file)"""
