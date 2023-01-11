import random
from pandas import Interval
from portento import Link


def create_link(link_type, interval, u, v):
    return link_type(interval=interval, u=u, v=v)


def generate_random_links(n, t_range, u_range, link_type=Link):
    for i in range(n):
        random_t = random.choice(t_range)
        random_delta = random.choice(range(1, 6))

        interval = Interval(random_t, random_t + random_delta, random.choice(['left', 'right', 'both', 'neither']))

        u = random.choice(u_range)
        v = u
        while v == u:
            v = random.choice(u_range)

        yield create_link(link_type=link_type, interval=interval, u=u, v=v)


def generate_stream(stream_type, link_type, s, n_links=200, t_range=range(50), u_range=range(12)):
    random.seed(s)
    return stream_type(list(generate_random_links(n_links, t_range, u_range, link_type)))
