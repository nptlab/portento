from pandas import Interval
from portento import DiStream, DiLink
from portento.algorithms.min_temporal_paths.latest_departure import latest_departure_time

latest_departure_time(DiStream([DiLink(Interval(0, 3), 0, 1), DiLink(Interval(0, 3), 0, 2)]), 0)
print("==")
latest_departure_time(DiStream([DiLink(Interval(0, 3), 0, 1)]), 0)
