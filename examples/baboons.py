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

"""def import_rfid_df(delta=20):
    df = pd.read_csv(THIS_FILE, compression='gzip', header=0, sep='\t')
    df = df.drop('DateTime', axis=1)
    delta = 20
    offset = df.iloc[0]['t']
    df['t'] = df['t'].map(lambda x: pd.Interval(x - offset, x - offset + delta, 'both'))
    return df


baboon_df = import_obs_df()
baboons = sorted(pd.unique(baboon_df[['i', 'j']].values.ravel('K')))
print("* The name of the baboons present:")
print(baboons)
"""


def import_obs_df():
    df = pd.read_csv(THIS_FILE, compression='gzip', header=0, sep='\t')
    df = df.drop(['Point', 'Behavior', 'Category'], axis=1)
    df = df.dropna()
    df = df.rename(columns={"DateTime": 't'})
    df = df.loc[df["Actor"] != df["Recipient"]]
    offset = pd.Timestamp(df.iloc[0]['t'])
    df['t'] = df['t'].map(lambda x: int((pd.Timestamp(x) - offset) / pd.Timedelta('1s')))
    df['t'] = list(map(lambda x: pd.Interval(x[0], sum(x), 'both'), df[['t', 'Duration']].itertuples(index=False)))
    return df


baboon_df = import_obs_df()
print(baboon_df[:10])

if not path.exists(STREAM_PICKLE):
    di_stream = portento.from_pandas_distream(baboon_df, interval='t', source="Actor", target="Recipient",
                                              instant_duration=0)
    pickle.dump(di_stream, open(STREAM_PICKLE, 'wb'))
else:
    di_stream = pickle.load(open(STREAM_PICKLE, 'rb'))


print("\n* A directed stream, where the order of nodes is relevant:")
print('\n'.join([str(link) for link in di_stream][:10]))

# TODO keep this
"""THIS_FILE = path.join(DATA_DIR, TXT_DIR, OBSERVE_FILE)
df = pd.read_csv(THIS_FILE, compression='gzip', header=0, sep='\t')
print(len(df))
df = df.drop(['Point', 'Behavior', 'Category'], axis=1)
df = df.dropna()
df = df.loc[df.Duration > 0]
df = df.rename(columns={"DateTime": "t"})
offset = pd.Timestamp(df.iloc[0]['t'])
df['t'] = df['t'].map(lambda x: int((pd.Timestamp(x) - offset) / pd.Timedelta('1s')))
df['t'] = df['t'] + df['Duration']
print(len(df))"""
