class PortentoException(Exception):
    """Base exception class"""


class PortentoIntervalException(PortentoException):
    """Exception concerning the usage of pandas intervals"""


class PortentoMergeException(PortentoIntervalException):
    """Raised when two non-overlapping intervals are given as input to merge_interval"""
