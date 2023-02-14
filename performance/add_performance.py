import random
from multiprocessing import Pool
from os import makedirs, remove, listdir
from pickle import dump, load
from timeit import Timer
from itertools import product
from utils import *
from setup import *

import portento


def generate_link(rnd, nodes, perc_mean_length, time_bound):
    u = rnd.choice(nodes)
    v = u
    while v == u:
        v = rnd.choice(nodes)

    mean_length = round((perc_mean_length * (time_bound.right - time_bound.left)) / 100)
    interval_length = min(max(round(rnd.normalvariate(mean_length, mean_length / 2)), 0), mean_length * 2)

    start_t = round(rnd.uniform(time_bound.left, time_bound.right - interval_length + 0.001))
    end_t = start_t + interval_length

    return portento.Link(pd.Interval(start_t, end_t, 'both'), u, v)


def performance_add(seed, n_nodes, perc_mean_int_len):
    desc_str = descriptive_str(n_nodes, perc_mean_int_len, seed)
    filename_performance = path.join(ADD_PERFORMANCE_PATH, desc_str)

    setup = SETUP_ADD

    if not path.exists(filename_performance):
        filename_stream = path.join(STREAM_PICKLE_PATH, desc_str)
        setup = "; ".join([setup, f'stream = load(open("{filename_stream}", "rb"))'])

        performances = []
        rnd = random.Random(seed)
        stream = portento.Stream()

        for i in range(1, MAX_LINKS + 1):
            link = generate_link(rnd, range(n_nodes), perc_mean_int_len, TIME_BOUND)

            if i % MEASUREMENT_MILESTONE == 0:
                dump(stream, open(filename_stream, "wb"))
                setup = "; ".join([setup, f'link = {str(link)}'])
                performances.append(Timer(ADD_COMMAND, setup).timeit(CMD_REP) / CMD_REP)
                stream = load(open(filename_stream, "rb"))

            stream.add(link)

        df = create_df(n_nodes, perc_mean_int_len, seed, performances)

        dump(df, open(filename_performance, "wb"))


def merge_results(n_nodes, perc_mean_int_len):
    desc_str = str_nodes_perc(n_nodes, perc_mean_int_len)
    if not path.exists(ADD_PERFORMANCE_PATH):
        raise Exception("Performance evaluations missing!")

    df = create_df(n_nodes, perc_mean_int_len, -1)

    used = []
    for eval_file in listdir(ADD_PERFORMANCE_PATH):
        if desc_str in eval_file:
            filepath = path.join(ADD_PERFORMANCE_PATH, eval_file)
            df = pd.concat([df, load(open(filepath, 'rb'))], axis=1)
            used.append(filepath)

    df = df.drop((n_nodes, perc_mean_int_len, -1), axis=1)
    filepath = path.join(ADD_PERFORMANCE_PATH, descriptive_str_from_substr(desc_str))
    dump(df, open(filepath, 'wb'))
    for u in used:
        remove(u)

    return df


if __name__ == "__main__":
    if not path.exists(STREAM_PICKLE_PATH):
        makedirs(STREAM_PICKLE_PATH)

    if not path.exists(ADD_PERFORMANCE_PATH):
        makedirs(ADD_PERFORMANCE_PATH)

    n_run = range(TEST_REP * len(N_NODES) * len(PERC_MEAN_INT_LEN))  # a different seed for each combination
    combos = product(range(TEST_REP), N_NODES, PERC_MEAN_INT_LEN)  # n, n_nodes, perc_mean_int_len
    combos = [(n_run[i], n_nodes, perc) for i, (_, n_nodes, perc) in enumerate(combos)]  # seed, n_nodes,
    # perc_mean_int_len
    with Pool() as pool:
        pool.starmap(performance_add, combos)

    combos = set(map(lambda x: x[1:], combos))
    with Pool() as pool:
        pool.starmap(merge_results, combos)

    print(merge_results(0, 0))
