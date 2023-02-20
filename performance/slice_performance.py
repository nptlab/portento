import random
from multiprocessing import Pool, cpu_count
from os import makedirs, listdir, remove
from pickle import load, dump
from itertools import product
from timeit import Timer
from more_itertools import chunked

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


def merge_results(*args):
    range_files = list(args)
    head = range_files[0]
    used = []

    def load_track(fp):
        used.append(fp)
        print(fp)
        return load(open(fp, 'rb'))

    res = pd.concat((load_track(path.join(SLICE_PERFORMANCE_PATH, f"stream-s_{s}")) for s in range_files), axis=0)
    dump(res, open(path.join(SLICE_PERFORMANCE_PATH, f"stream-s_{head}_1"), 'wb'))

    for eval_file in used:
        remove(eval_file)


if __name__ == "__main__":
    """if not path.exists(STREAM_PICKLE_PATH):
        raise Exception("Error: create streams before. Run add_performance.main")

    if not path.exists(SLICE_PERFORMANCE_PATH):
        makedirs(SLICE_PERFORMANCE_PATH)

    with Pool() as pool:
        pool.starmap(performance_slice, combos)

    n_evals = len(listdir(SLICE_PERFORMANCE_PATH))
    chunked_evals = chunked(range(n_evals), n_evals // cpu_count())

    with Pool() as pool:
        pool.starmap(merge_results, chunked_evals)"""

    """df = pd.concat(map(lambda x: load(open(path.join(SLICE_PERFORMANCE_PATH, x), 'rb')),
                       listdir(SLICE_PERFORMANCE_PATH)))

    for x in listdir(SLICE_PERFORMANCE_PATH):
        remove(path.join(SLICE_PERFORMANCE_PATH, x))"""

    df = load(open(path.join(SLICE_PERFORMANCE_PATH, "stream-tot"), 'rb'))

    for n, i in product(PERC_NODES, PERC_INTERVAL):
        df[(n, i, 'diff')] = df[(n, i, 'time')] - df[(n, i, 'node')]

    df = df.drop(['node', 'time'], axis=1, level=2)
    df = df.droplevel(level=2, axis=1)
    df = df.quantile([0, .25, .5, .75, 1], axis=0)

    print(df[25])
    print(df[50])
    print(df[75])
