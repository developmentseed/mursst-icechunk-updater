"""Custom exceptions for MURSST Icechunk Updater."""


class MursstUpdaterError(Exception):
    """Base exception for all MURSST updater errors."""

    pass


class GranuleSearchError(MursstUpdaterError):
    """Base exception for all issues related to granule search"""


class DateOrderError(GranuleSearchError):
    """Raise when the start and end date are not monotonic"""


class NoNewDataError(GranuleSearchError):
    """Raised when no new data granules are available for processing."""

    pass
