"""
Provide slice views for the Stream class.

Slices can be over nodes, time or both (compounded).
Slices can't be modified but are updated when the Stream is updated.

"""
from copy import copy
from collections.abc import Hashable

from .stream import Stream
from .filters import no_filter


# TODO slices will be a "filter" over the methods of the Stream.
# TODO 'nbunch' will be a boolean function that selects nodes
# TODO check intersection of red-black trees

class Slice:

    def __init__(self, stream: Stream = Stream(), node_filter=no_filter, time_filter=no_filter):
        self._stream = stream
        self._node_filter = node_filter
        self._time_filter = time_filter
        self._compound_filter = lambda link: all([self._node_filter(link.u),
                                                  self._node_filter(link.v),
                                                  self._time_filter(link.interval)])

    def __iter__(self):
        return filter(self._compound_filter, self._stream)

    def __contains__(self, item):
        return self._node_filter(item) and item in self._stream.dict_view.nodes

    def __getitem__(self, item):
        if isinstance(item, tuple):
            if all([self._node_filter(node) for node in item]):
                return filter(self._time_filter, self._stream.dict_view[item])
            else:
                raise Exception

        elif isinstance(item, Hashable):
            if self._node_filter(item):
                return filter(self._time_filter, self._stream.dict_view[item])
            else:
                raise Exception

        else:
            raise Exception

    def to_stream(self):
        stream = copy(self._stream)
