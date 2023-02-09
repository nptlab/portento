from portento.utils import *


@dataclass
class StreamTreeNode(IntervalTreeNode):
    u: Hashable = field(default=None, compare=True)
    v: Hashable = field(default=None, compare=True)

    def __post_init__(self):
        self.full_interval = self.value
        self.instant_duration = None
        self.time_instants = None

    def __iter__(self):
        if self.left:
            yield from iter(self.left)
        yield Link(self.value, self.u, self.v)
        if self.right:
            yield from iter(self.right)

    def __str__(self):
        return f"{self.value, self.u, self.v}"

    @property
    def length(self):
        raise NotImplementedError("This metric has no meaning in this data structure.")

    def is_left(self):
        return super().is_left() and \
               self.u == self.parent.left.u and \
               self.v == self.parent.left.v

    def get_sibling(self):
        if self.parent:
            return self.parent.right if self.is_left() else self.parent.left
        else:
            return None

    def _compute_time_instants(self):
        pass

    def _update_time_instants_add(self):
        pass

    def _update_time_instants_delete(self):
        pass

    def overlaps(self, other):
        if (not other.u and not other.v) or (not self.u and not self.v):  # just overlap over the interval
            return self.value.overlaps(other.value)
        else:
            return self.value.overlaps(other.value) and self.u == other.u and self.v == other.v

    def _merge_values(self, other):
        return StreamTreeNode(merge_interval(self.value, other.value), u=self.u, v=self.v)

    def _slice_cut(self, other):
        return Link(cut_interval(self.value, other.value), u=self.u, v=self.v)


@dataclass
class DiStreamTreeNode(StreamTreeNode):

    def __iter__(self):
        if self.left:
            yield from iter(self.left)
        yield DiLink(self.value, self.u, self.v)
        if self.right:
            yield from iter(self.right)


class StreamTree(IntervalTree):
    """The data structure for stream graphs that allows time-based slices

    """

    @classmethod
    def _create_node(cls, data, instant_duration):
        # data is assumed to be a link
        if isinstance(data, Interval):
            return StreamTreeNode(data)
        elif isinstance(data, Link):
            return StreamTreeNode(value=data.interval, u=data.u, v=data.v)

    @classmethod
    def _merge(cls, node_1, node_2):
        if node_1.u == node_1.u and node_1.v == node_2.v:
            return StreamTreeNode(merge_interval(node_1.value, node_2.value), u=node_1.u, v=node_1.v)
        else:
            raise AttributeError("Two nodes must have same u and v")


class DiStreamTree(StreamTree):
    """The data structure for directed stream graphs that allows time-based slices

    """

    @classmethod
    def _create_node(cls, data, instant_duration):
        # data is assumed to be a link
        if isinstance(data, Interval):
            return DiStreamTreeNode(data)
        elif isinstance(data, DiLink):
            return DiStreamTreeNode(value=data.interval, u=data.u, v=data.v)

    @classmethod
    def _merge(cls, node_1, node_2):
        if node_1.u == node_1.u and node_1.v == node_2.v:
            return DiStreamTreeNode(merge_interval(node_1.value, node_2.value), u=node_1.u, v=node_1.v)
        else:
            raise AttributeError("Two nodes must have same u and v")


