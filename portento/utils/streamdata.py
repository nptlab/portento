from dataclasses import dataclass, field
from collections.abc import Hashable

from pandas import Interval

from .intervaltree import IntervalTree
from .sortstreamnodes import sort_nodes


@dataclass(frozen=True, order=True)
class Link:
    """Base dataclass containing all the information of a link of the stream.

    Parameters
    ----------
    interval : Interval
    u : node (Hashable)
    v : node (Hashable)

    """
    interval: Interval
    u: Hashable = field(compare=False)
    v: Hashable = field(compare=False)

    def __post_init__(self):
        if self.u is None or self.v is None:
            raise ValueError(f"Tried to create a link with a node equal to {None} "
                             f"in interval {self.interval}")
        if not isinstance(self.u, Hashable) or not isinstance(self.v, Hashable):
            raise TypeError("Tried to create a link with non-hashable node.")

        u, v = sort_nodes([self.u, self.v])
        object.__setattr__(self, 'u', u)
        object.__setattr__(self, 'v', v)

    def __iter__(self):
        return iter((self.interval, self.u, self.v))


@dataclass(frozen=True)
class DiLink(Link):
    """Base dataclass containing all the information of a directed link of the directed stream.
    This works like the Link, just it doesn't sort nodes.

        Parameters
        ----------
        interval : Interval
        u : node (Hashable)
        v : node (Hashable)

        """

    def __post_init__(self):
        if self.u is None or self.v is None:
            raise ValueError(f"Tried to create a link with a node equal to {None} "
                             f"in interval {self.interval}")
        if not isinstance(self.u, Hashable) or not isinstance(self.v, Hashable):
            raise TypeError("Tried to create a link with non-hashable node.")


class IntervalContainer:
    """The container class for data in the stream.
    This class contains one of the below:
    - All the time intervals in which a node is active (a node has at least an active link)
    - All the time intervals in which an edge is active (two nodes have an active link)
    Overlapping time intervals are merged when the function add(link) is called

    This class is used as link_container_factory inside the StreamDict class.

    """
    intervals_container_factory = IntervalTree

    def __init__(self, *args, instant_duration=1):
        if (len(args) == 1 or len(args) == 2) and all([isinstance(node, Hashable) for node in args]):
            self._cond = self.__class__()._initialize_cond(args)
        elif len(args) == 0:
            self._cond = None
        else:
            raise AttributeError

        self._intervals = self.intervals_container_factory(instant_duration=instant_duration)

    def __iter__(self):
        if len(self._cond) == 2:
            u, v = self._cond
            return iter(Link(interval, u, v) for interval in self._intervals)
        else:
            return iter(self._intervals)

    @property
    def interval_tree(self):
        return self._intervals

    @property
    def length(self):
        return self._intervals.length

    def add(self, link):
        """Add an interval to the Intervals if the link respects the condition

        Parameters
        ----------
        link : Link
            The link to insert.

        Returns
        -------
        result : Bool
            Whether the link was inserted or not.

        """
        interval, u, v = link

        try:
            # check condition
            if len(self._cond) == 1:
                reference_node = self._cond[0]
                if repr(u) == repr(reference_node) or repr(v) == repr(reference_node):
                    self._add(interval)
                else:
                    return False
            elif len(self._cond) == 2:
                reference_u, reference_v = self._cond
                if repr(u) == repr(reference_u) and repr(v) == repr(reference_v):
                    self._add(interval)
                else:
                    return False
        except TypeError:
            if self._cond is None:  # initialize the condition
                self._cond = (u, v)
                return self.add(link)

        return True

    def _add(self, interval):
        self._intervals.add(interval)

    @classmethod
    def _initialize_cond(cls, args):
        return sort_nodes(args)


class DiIntervalContainer(IntervalContainer):
    """The container class for data in the directed stream.
    This class contains one of the below:
    - All the time intervals in which a node is active (a node has at least an active link)
    - All the time intervals in which an edge is active (two nodes have an active link)
    Overlapping time intervals are merged when the function add(link) is called

    This class is used as link_container_factory inside the StreamDict class.

    It differs from the IntervalContainer in the fact that it doesn't sort nodes, as links are directed.

    """

    @classmethod
    def _initialize_cond(cls, args):
        return args
