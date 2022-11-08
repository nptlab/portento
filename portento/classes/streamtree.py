from typing import Optional

from pandas import Interval
from dataclasses import dataclass, field
from collections.abc import Hashable

from portento.utils import *


@dataclass
class StreamTreeNode(IntervalTreeNode):
    u: Hashable = field(default=None, compare=False)
    v: Hashable = field(default=None, compare=False)

    def __post_init__(self):
        self.full_interval = self.value
        self.time_instants = None

    def __iter__(self):
        if self.left:
            yield from iter(self.left)
        yield Link(self.value, self.u, self.v)
        if self.right:
            yield from iter(self.right)

    def __str__(self):
        return str(super().__str__(), str(self.u), str(self.v))

    @property
    @property
    def length(self):
        raise NotImplementedError("This metric has no meaning in this data structure."
                                  "If you need the total duration of a Stream, call stream_duration_len()"
                                  "of the Stream object.")

    def overlaps(self, other):
        if (not other.u and not other.v) or (not self.u and not self.v):  # just overlap over the interval
            return self.value.overlaps(other.value)
        else:
            return self.value.overlaps(other.value) and self.u == other.u and self.v == other.v

    def _update_time_instants(self):
        pass

    def _merge_values(self, other):
        return StreamTreeNode(merge_interval(self.value, other.value), u=self.u, v=self.v)

    def _slice_cut(self, other):
        return Link(cut_interval(self.value, other.value), u=self.u, v=self.v)


class StreamTree(IntervalTree):
    """The data structure for stream graphs that allows time-based slices

    """
    value_type = Link

    @classmethod
    def _create_node(cls, data):
        # data is assumed to be a link
        if isinstance(data, Interval):
            return StreamTreeNode(data)
        elif isinstance(data, Link):
            return StreamTreeNode(data.interval, u=data.u, v=data.v)
