from os import path
import pickle
import pandas as pd

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

KENYA_FINAL_COLUMNS = ['m1', 'm2', 't']


def import_kenya_data_as_df():
    kenya_a = pd.read_csv(KENYA_FILE_FULL_A, header=0, sep=',')
    kenya_w = pd.read_csv(KENYA_FILE_FULL_W, header=0, sep=',')
    kenya_w = kenya_w[kenya_w['h1'] != 'H' and kenya_w['h1'] != 'B']
    # kenya = kenya_w.append(kenya_a)
    print(kenya_w)
    raise Exception

    kenya_a = kenya_a.drop(set(kenya_a.columns) - {'m1', 'm2', 'day', 'hour'}, axis=1)
    kenya_w = kenya_w.drop(set(kenya_w.columns) - {'m1', 'm2', 'day', 'hour'}, axis=1)

    kenya_w['t'] = (24 * kenya_w['day']) + kenya_w['hour']
    kenya_w.t = kenya_w.t.apply(lambda x: pd.Interval(x, x, 'both'))
    print(kenya_w)
    raise Exception


if __name__ == "__main__":
    import_kenya_data_as_df()
    malawi_df = import_malawi_data_as_df()

    if not path.exists(MALAWI_STREAM_PICKLE_FULL):
        stream = portento.from_pandas_stream(malawi_df,
                                             *MALAWI_FINAL_COLUMNS)
        pickle.dump(stream, open(MALAWI_STREAM_PICKLE_FULL, 'wb'))
    else:
        stream = pickle.load(open(MALAWI_STREAM_PICKLE_FULL, 'rb'))

    print(malawi_df[:10])

    print(portento.card_V(stream))
    print(portento.card_W(stream))
    print(portento.card_E(stream))
    print(portento.card_T(stream))

    print(min_temporal_paths.earliest_arrival_time(stream, 71))
    print(min_temporal_paths.latest_departure_time(stream, 71))
    print(min_temporal_paths.shortest_path_distance(stream, 71))
    print(min_temporal_paths.fastest_path_duration(stream, 71))
    print(min_temporal_paths.fastest_path_duration_multipass(stream, 71))

