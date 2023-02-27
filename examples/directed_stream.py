"""
======================
DiStream
======================

Presenting the basic usage of a directed Stream

Data was taken from: http://www.sociopatterns.org/datasets/baboons-interactions/.
"""
from os import path
import pandas as pd
import pickle

import portento

DATA_DIR = path.join('sociopatterns', 'data')
TXT_DIR = 'txt'
PICKLE_DIR = 'pickled_stream'

SENSOR_FILE = 'RFID_data.txt.gz'
SENSOR_STREAM_PICKLE = 'rfid_baboons_stream'

OBSERVE_FILE = 'OBS_data.txt.gz'
OBSERVE_STREAM_PICKLE = 'obs_baboons_stream'

THIS_FILE = path.join(DATA_DIR, TXT_DIR, OBSERVE_FILE)
STREAM_PICKLE = path.join(DATA_DIR, PICKLE_DIR, OBSERVE_STREAM_PICKLE)


def import_obs_df():
    df = pd.read_csv(THIS_FILE, compression='gzip', header=0, sep='\t')
    df = df.drop(['Point', 'Behavior', 'Category'], axis=1)
    df = df.dropna()
    df = df.rename(columns={"DateTime": 't'})
    df = df.loc[df["Actor"] != df["Recipient"]]
    offset = pd.Timestamp(df['t'].min())
    df['Actor'] = df['Actor'].map(lambda x: x.strip())
    df['Recipient'] = df['Recipient'].map(lambda x: x.strip())
    df['t'] = df['t'].map(lambda x: int((pd.Timestamp(x) - offset) / pd.Timedelta('1s')))
    df['t'] = list(map(lambda x: pd.Interval(x[0], sum(x), 'both'), df[['t', 'Duration']].itertuples(index=False)))
    return df


baboon_df = import_obs_df()
baboons = pd.unique(baboon_df[["Actor", "Recipient"]].values.ravel('K'))
print(baboons)
print(baboon_df[:10])

if not path.exists(STREAM_PICKLE):
    di_stream = portento.from_pandas_di_stream(baboon_df, interval='t', source="Actor", target="Recipient",
                                               instant_duration=0)
    pickle.dump(di_stream, open(STREAM_PICKLE, 'wb'))
else:
    di_stream = pickle.load(open(STREAM_PICKLE, 'rb'))

print("\n* A directed stream, where the order of nodes is relevant:")
print('\n'.join([str(link) for link in di_stream][:10]))

print("\n* A directed stream, where the order of nodes is relevant:")
print('\n'.join([str(link) for link in di_stream[('MUSE', 'NEKKE')]][:10]))
print("======")
print('\n'.join([str(link) for link in di_stream[('NEKKE', 'MUSE')]][:10]))

print("\n* A DiStream can be sliced as a stream:")
filtered_di_stream = list(portento.slice_di_stream(di_stream,
                                                   portento.NodeFilter(lambda node: node in ['MUSE', 'NEKKE']),
                                                   portento.TimeFilter([pd.Interval(2600000, 7796100)])))
print('\n'.join([str(link) for link in filtered_di_stream][:10]))
print("=======")
filtered_di_stream = list(portento.slice_di_stream(di_stream,
                                                   portento.NodeFilter(lambda node: node in ['MUSE', 'NEKKE']),
                                                   portento.TimeFilter([pd.Interval(2600000, 7796100)]), first='node'))
print('\n'.join([str(link) for link in filtered_di_stream][:10]))

print("\n* A DiStream can be exported as pandas DataFrame:")
print(portento.to_pandas_stream(portento.DiStream(filtered_di_stream)))
