from os import path, makedirs
from multiprocessing import Pool
import pickle
import random
from timeit import Timer
from itertools import repeat, chain
import pandas as pd
from setup import *

import portento
from portento import min_temporal_paths

DATA_DIR = 'path_data'

MALAWI_CSV_DIR = 'malawi'
PICKLE_DIR = 'pickled_stream'
MALAWI_FILE = 'tnet_malawi_pilot.malawi.gz'
MALAWI_STREAM_PICKLE = 'malawi_stream'

MALAWI_FILE_FULL = path.join(DATA_DIR, MALAWI_CSV_DIR, MALAWI_FILE)
MALAWI_STREAM_PICKLE_FULL = path.join(DATA_DIR, PICKLE_DIR, MALAWI_STREAM_PICKLE)

MALAWI_FINAL_COLUMNS = ['t', 'i', 'j']


def import_malawi_data_as_df():
    malawi = pd.read_csv(MALAWI_FILE_FULL, compression='gzip', header=0, sep=',', index_col=0)
    malawi = malawi.drop('day', axis=1)
    malawi.columns = MALAWI_FINAL_COLUMNS
    malawi.t = malawi.t.apply(lambda x: pd.Interval(x, x, 'both'))
    return malawi


KENYA_CSV_DIR = 'kenya'
KENYA_FILE_A = 'scc2034_kilifi_all_contacts_across_households.csv'
KENYA_FILE_W = 'scc2034_kilifi_all_contacts_within_households.csv'
KENYA_STREAM_PICKLE = 'kenya_stream'

KENYA_FILE_FULL_A = path.join(DATA_DIR, KENYA_CSV_DIR, KENYA_FILE_A)
KENYA_FILE_FULL_W = path.join(DATA_DIR, KENYA_CSV_DIR, KENYA_FILE_W)
KENYA_STREAM_PICKLE_FULL = path.join(DATA_DIR, PICKLE_DIR, KENYA_STREAM_PICKLE)

KENYA_FINAL_COLUMNS = ['t', 'm1', 'm2']


def import_kenya_data_as_df():
    kenya_a = pd.read_csv(KENYA_FILE_FULL_A, header=0, sep=',')
    kenya_w = pd.read_csv(KENYA_FILE_FULL_W, header=0, sep=',')
    kenya_w = kenya_w[(kenya_w.h1 != 'H') & (kenya_w.h1 != 'B')]  # made in other days
    kenya = kenya_w.append(kenya_a)

    kenya = kenya.query("m1 != m2")  # drop self loops
    kenya = kenya.drop(set(kenya.columns) - {'m1', 'm2', 'day', 'hour'}, axis=1)

    kenya['t'] = (24 * kenya['day']) + kenya['hour']
    kenya.t = kenya.t.apply(lambda x: pd.Interval(x, x, 'both'))

    return kenya


def load_or_create_and_dump(stream_path, cols, func=None):
    if not path.exists(stream_path):
        df = func()
        print(df)
        stream = portento.from_pandas_stream(df,
                                             *cols)
        pickle.dump(stream, open(stream_path, 'wb'))
    else:
        stream = pickle.load(open(stream_path, 'rb'))

    return stream


def cardinalities(stream):
    return dict(zip(('T', 'V', 'W', 'E'), (portento.card_T(stream),
                                           portento.card_V(stream),
                                           portento.card_W(stream),
                                           portento.card_E(stream))))


def performance_path(node, filename_stream):
    stream_name = filename_stream.split('\\')[-1]
    res_dump = path.join(PATH_PERFORMANCE_PATH, f"{stream_name}-{node}")

    if not path.exists(res_dump):
        setup = "; ".join([SETUP_PATH,
                           f'stream = load(open("{filename_stream}", "rb"))',
                           f'node={node}'])

        df = pd.DataFrame(dict(zip(PATH_NAME,
                                   ([Timer(cmd, setup).timeit(CMD_REP_PATH) / CMD_REP_PATH]
                                    for cmd in PATH_COMMAND))))
        df.columns = pd.MultiIndex.from_product([[stream_name], df.columns])
        pickle.dump(df, open(res_dump, "wb"))


if __name__ == "__main__":

    if not path.exists(PATH_PERFORMANCE_PATH):
        makedirs(PATH_PERFORMANCE_PATH)

    malawi_stream = load_or_create_and_dump(MALAWI_STREAM_PICKLE_FULL,
                                            MALAWI_FINAL_COLUMNS, import_malawi_data_as_df)

    kenya_stream = load_or_create_and_dump(KENYA_STREAM_PICKLE_FULL,
                                           KENYA_FINAL_COLUMNS, import_kenya_data_as_df)

    print(cardinalities(malawi_stream))
    print(cardinalities(kenya_stream))

    rnd = random.Random(0)
    malawi_nodes = zip(rnd.sample(portento.V(malawi_stream), N_NODES_PATH), repeat(MALAWI_STREAM_PICKLE_FULL))
    kenya_nodes = zip(rnd.sample(portento.V(kenya_stream), N_NODES_PATH), repeat(KENYA_STREAM_PICKLE_FULL))

    with Pool() as pool:
        pool.starmap(performance_path, [list(chain(malawi_nodes, kenya_nodes))[0]])

    print(pickle.load(open('path_performance_res/malawi_stream-53', 'rb')))
