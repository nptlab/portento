from pandas import Interval
from portento import Link


def generate_link(rnd, nodes, perc_mean_length, time_bound):
    u = rnd.choice(nodes)
    v = u
    while v == u:
        v = rnd.choice(nodes)

    mean_length = round((perc_mean_length * (time_bound.right - time_bound.left)) / 100)
    interval_length = min(max(round(rnd.normalvariate(mean_length, mean_length / 2)), 0), mean_length * 2)

    start_t = round(rnd.uniform(time_bound.left, time_bound.right - interval_length + 0.001))
    end_t = start_t + interval_length

    return Link(Interval(start_t, end_t, 'both'), u, v)
