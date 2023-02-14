import random
from multiprocessing import Pool
from os import makedirs, remove, listdir
from pickle import dump, load
from timeit import Timer
from itertools import product
from utils import *
from setup import *


def performance_path(seed, n_nodes, perc_mean_int_len):
    desc_str = descriptive_str(n_nodes, perc_mean_int_len, seed)
    filename_performance = path.join(PATH_PERFORMANCE_PATH, f"stream-s_{seed}")

    setup = SETUP_PATH

    if not path.exists(filename_performance):
        filename_stream = path.join(STREAM_PICKLE_PATH, desc_str)
        setup = "; ".join([setup, f'stream = load(open("{filename_stream}", "rb"))'])

        rnd = random.Random(seed)
        nodes = list(load(open(filename_stream, 'rb')).nodes)

        dfs = []
        filepath = path.join(PATH_PERFORMANCE_PATH, f"stream-s_{seed}")

        for cmd, name in zip(PATH_COMMAND, PATH_NAMES):
            performances = []
            for n in nodes_subset_n(rnd, N_NODES_PATH, nodes):
                print("node:", n)
                perf_setup = "; ".join([setup, f'node={n}'])
                print("doing:", cmd)
                performances.append(Timer(cmd, perf_setup).timeit(CMD_REP_PATH) / CMD_REP_PATH)
                print("done:", cmd)

            df = pd.DataFrame(performances, columns=[name])
            print(df)
            dfs.append(df)

        df = pd.concat(dfs, axis=1)
        dump(df, open(filepath, 'wb'))


if __name__ == "__main__":
    if not path.exists(PATH_PERFORMANCE_PATH):
        makedirs(PATH_PERFORMANCE_PATH)

    """n_run = range(TEST_REP * len(N_NODES) * len(PERC_MEAN_INT_LEN))  # a different seed for each combination
    combos = product(range(TEST_REP), N_NODES, PERC_MEAN_INT_LEN)  # n, n_nodes, perc_mean_int_len
    combos = [(n_run[i], n_nodes, perc) for i, (_, n_nodes, perc) in enumerate(combos)]  # seed, n_nodes,
    # perc_mean_int_len"""

    performance_path(1, 100, 0.1)
