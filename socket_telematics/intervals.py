from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from .exceptions import ConfigError


@dataclass(frozen=True)
class IntervalConfig:
    """Per-client send interval configuration."""

    intervals_s: dict[str, float]

    def interval_for(self, client_id: str, default_s: float) -> float:
        value = self.intervals_s.get(client_id)
        return default_s if value is None else value


_CLIENT_LINE_RE: Final[re.Pattern[str]] = re.compile(
    r"^\s*(?P<client_id>[A-Za-z0-9_\-]+)\s*[=:]\s*(?P<interval>\d+(?:\.\d+)?)\s*(?:s|sec|secs|seconds)?\s*$"
)


def load_intervals(path: str | Path) -> IntervalConfig:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Interval config file not found: {p}")
    if not p.is_file():
        raise ConfigError(f"Interval config path is not a file: {p}")

    suffix = p.suffix.lower()
    try:
        if suffix == ".xml":
            return _load_intervals_xml(p)
        return _load_intervals_text(p)
    except ConfigError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise ConfigError(f"Failed to load interval config '{p}': {exc}") from exc


def _load_intervals_text(path: Path) -> IntervalConfig:
    intervals: dict[str, float] = {}

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ConfigError(f"Unable to read interval config '{path}': {exc}") from exc

    for idx, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue

        match = _CLIENT_LINE_RE.match(raw)
        if not match:
            raise ConfigError(
                "Invalid interval line format at "
                f"{path}:{idx}. Expected 'CLIENT_ID=1.5' (seconds)."
            )

        client_id = match.group("client_id")
        interval_s = float(match.group("interval"))
        if interval_s <= 0:
            raise ConfigError(f"Interval must be > 0 at {path}:{idx}")

        intervals[client_id] = interval_s

    if not intervals:
        raise ConfigError(f"No client intervals found in '{path}'")

    return IntervalConfig(intervals_s=intervals)


def _load_intervals_xml(path: Path) -> IntervalConfig:
    try:
        root = ET.fromstring(path.read_text(encoding="utf-8"))
    except ET.ParseError as exc:
        raise ConfigError(f"Invalid XML in '{path}': {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"Unable to read interval config '{path}': {exc}") from exc

    intervals: dict[str, float] = {}

    # Expected shape:
    # <clients>
    #   <client id="CAR_101" interval="1.0" />
    # </clients>
    for node in root.findall(".//client"):
        client_id = node.attrib.get("id")
        interval_raw = node.attrib.get("interval")

        if not client_id or not interval_raw:
            raise ConfigError(f"Each <client> must have 'id' and 'interval' attributes in '{path}'")

        try:
            interval_s = float(interval_raw)
        except ValueError as exc:
            raise ConfigError(f"Invalid interval '{interval_raw}' for client '{client_id}' in '{path}'") from exc

        if interval_s <= 0:
            raise ConfigError(f"Interval must be > 0 for client '{client_id}' in '{path}'")

        intervals[client_id] = interval_s

    if not intervals:
        raise ConfigError(f"No <client> intervals found in '{path}'")

    return IntervalConfig(intervals_s=intervals)
