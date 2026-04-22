from __future__ import annotations


class SocketTelematicsError(Exception):
    """Base exception for the socket_telematics project."""


class ConfigError(SocketTelematicsError):
    """Raised when configuration files are missing/invalid."""


class ProtocolError(SocketTelematicsError):
    """Raised when a message cannot be parsed or validated."""


class StorageError(SocketTelematicsError):
    """Raised when persistence (SQLite) operations fail."""
