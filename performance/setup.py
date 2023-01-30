from os import path
from itertools import accumulate, repeat
from operator import mul
from multiprocessing import Pool
from pandas import Interval

STREAM_PICKLE_PATH = path.join(".", "stream_pickle")
ADD_PERFORMANCE_PATH = path.join(".", "add_performance")
COMMAND = "stream.add(link)"
SETUP = "; ".join(['from pickle import load',
                   'from os.path import join',
                   'from pandas import Interval',
                   'from portento import Link'])
CMD_REP = 1000  # number of repetitions of the command. DEFAULT: 1000000

TEST_REP = 100  # repetition of the same test. DEFAULT: 100
MAX_LINKS = 20  # max number of links. DEFAULT: 10000
MEASUREMENT_MILESTONE = 5  # each n links, a test of performance is made. DEFAULT: 100
TIME_BOUND = Interval(0, 10000, 'both')  # time boundary of the stream. DEFAULT: Interval(0, 10000, 'both')

N_NODES = list(map(lambda x: 100 * x, [1, 5]))  # number of nodes in the stream.
# DEFAULT: list(map(lambda x: 100 * x, [1, 5, 10, 50]))
PERC_MEAN_INT_LEN = [0.001 * i for i in accumulate(repeat(100, 2), mul)]  # percentage of the length of a link
# based on TIME_BOUND. DEFAULT: [0.001 * i for i in accumulate(repeat(10, 4), mul)]
