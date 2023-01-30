import random
import logging
from os import makedirs, remove, listdir
from pickle import dump, load
from timeit import Timer
from itertools import product
from portento import Stream
from generate_link import generate_link
from utils import *
from setup import *

import pandas as pd

if not path.exists(STREAM_PICKLE_PATH):
    makedirs(STREAM_PICKLE_PATH)

if not path.exists(ADD_PERFORMANCE_PATH):
    makedirs(ADD_PERFORMANCE_PATH)

combos = list(product(range(TEST_REP), N_NODES, PERC_MEAN_INT_LEN))  # seed, n_nodes, perc_mean_int_len


def performance_test(seed, n_nodes, perc_mean_int_len):
    desc_str = descriptive_str(n_nodes, perc_mean_int_len, seed)
    filename_performance = path.join(ADD_PERFORMANCE_PATH, desc_str)

    setup = SETUP

    if not path.exists(filename_performance):
        logging.info(f"started eval. n nodes: {n_nodes}, perc mean: {perc_mean_int_len}, seed: {seed}")
        filename_stream = path.join(STREAM_PICKLE_PATH, desc_str)
        setup = "; ".join([setup, f'stream = load(open("{filename_stream}", "rb"))'])

        performances = []
        rnd = random.Random(seed)
        stream = Stream()

        for i in range(1, MAX_LINKS+1):
            link = generate_link(rnd, range(n_nodes), perc_mean_int_len, TIME_BOUND)

            if i % MEASUREMENT_MILESTONE == 0:
                logging.info(f"evaluating performance at milestone {i} for conf:"
                             f" n nodes: {n_nodes}, perc mean: {perc_mean_int_len}, seed: {seed}")
                dump(stream, open(filename_stream, "wb"))
                setup = "; ".join([setup, f'link = {str(link)}'])
                performances.append(Timer(COMMAND, setup).timeit(CMD_REP) / CMD_REP)
                stream = load(open(filename_stream, "rb"))
                logging.info(f"finished performance eval. at milestone {i} for conf:"
                             f" n nodes: {n_nodes}, perc mean: {perc_mean_int_len}, seed: {seed}")

            stream.add(link)

        logging.info(f"finished all performance eval. for conf:"
                     f" n nodes: {n_nodes}, perc mean: {perc_mean_int_len}, seed: {seed}")

        df = create_df(n_nodes, perc_mean_int_len, seed, performances)

        dump(df, open(filename_performance, "wb"))
    else:
        logging.warning(f"eval. already exists for conf:"
                        f" n nodes: {n_nodes}, perc mean: {perc_mean_int_len}, seed: {seed}")


def merge_results(n_nodes, perc_mean_int_len, condition="desc_str in eval_file"):
    desc_str = str_nodes_perc(n_nodes, perc_mean_int_len)
    if not path.exists(ADD_PERFORMANCE_PATH):
        raise Exception("Performance evaluations missing!")

    df = create_df(n_nodes, perc_mean_int_len, -1)

    used = []
    for eval_file in listdir(ADD_PERFORMANCE_PATH):
        if eval(condition):
            filepath = path.join(ADD_PERFORMANCE_PATH, eval_file)
            df = pd.concat([df, load(open(filepath, 'rb'))], axis=1)
            used.append(filepath)

    df = df.drop((n_nodes, perc_mean_int_len, -1), axis=1)
    # TODO some data modification here
    filepath = path.join(ADD_PERFORMANCE_PATH, descriptive_str_from_substr(desc_str))
    dump(df, open(filepath, 'wb'))
    for u in used:
        remove(u)

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.NOTSET)
    combos = list(product(range(TEST_REP), N_NODES, PERC_MEAN_INT_LEN))  # seed, n_nodes, perc_mean_int_len
    with Pool() as pool:
        pool.starmap(performance_test, combos)

    combos = set(map(lambda x: x[1:], combos))
    print(combos)
    with Pool() as pool:
        pool.starmap(merge_results, combos)

    print(merge_results(0, 0, "True"))
