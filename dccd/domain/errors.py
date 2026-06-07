"""Domain-level exceptions."""

__all__ = ["NoCapability", "CoverageError", "DccdError"]


class DccdError(Exception):
    """Base class for all dccd errors."""


class NoCapability(DccdError):
    """Raised when a source cannot satisfy a requested operation/data type."""

    def __init__(self, exchange: str, data_type: str, mode: str, reason: str = "") -> None:
        self.exchange = exchange
        self.data_type = data_type
        self.mode = mode
        msg = f"{exchange} has no {mode} capability for {data_type}"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)


class CoverageError(DccdError):
    """Raised when a source cannot cover the requested time window."""

    def __init__(self, exchange: str, data_type: str, start: int, available_from: int | None = None) -> None:
        self.exchange = exchange
        self.data_type = data_type
        self.start = start
        self.available_from = available_from
        msg = f"{exchange} {data_type} history does not go back to {start}"
        if available_from is not None:
            msg += f" (earliest available: {available_from})"
        super().__init__(msg)
