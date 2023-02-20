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
                perf_setup = "; ".join([setup, f'node={n}'])
                performances.append(Timer(cmd, perf_setup).timeit(CMD_REP_PATH) / CMD_REP_PATH)

            df = create_df(n_nodes, perc_mean_int_len, name, performances)
            dfs.append(df)

        df = pd.concat(dfs, axis=1)
        dump(df, open(filepath, 'wb'))


if __name__ == "__main__":
    if not path.exists(PATH_PERFORMANCE_PATH):
        makedirs(PATH_PERFORMANCE_PATH)

    """with Pool() as pool:
        pool.starmap(performance_path, combos)

    print(load(open('path_performance_res/stream-s_1394', 'rb')))"""

