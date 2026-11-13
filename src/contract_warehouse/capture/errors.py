"""Error taxonomy for the export-task capture client.

All failures raised by the capture client are :class:`CaptureError`
subclasses so the CLI can map them to a non-zero exit with a clear message.
The client never retries with a different identity and never swallows a
network/protocol failure by treating it as a 404.
"""

from __future__ import annotations


class CaptureError(ValueError):
    """Base class for every capture client failure."""


class EndpointError(CaptureError):
    """The configured endpoint is not an allowed loopback API base address."""


class ProtocolError(CaptureError):
    """The portal spoke outside the export protocol (HTTP, JSON, limits)."""


class StateError(CaptureError):
    """The local SQLite state is missing, foreign, inconsistent or locked."""


class BusinessValidationError(CaptureError):
    """The captured four-file snapshot fails the existing v2 business rules."""
