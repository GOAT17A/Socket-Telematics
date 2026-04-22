from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .exceptions import StorageError


@dataclass(frozen=True)
class StorageConfig:
    db_path: str


class SQLiteStorage:
    def __init__(self, config: StorageConfig) -> None:
        self._config = config
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

    def start(self) -> None:
        try:
            db_path = Path(self._config.db_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._init_schema()
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to start storage: {exc}") from exc

    def stop(self) -> None:
        with self._lock:
            if self._conn is None:
                return
            try:
                self._conn.close()
            finally:
                self._conn = None

    def _init_schema(self) -> None:
        assert self._conn is not None
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telemetry_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    received_at TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    speed_kph REAL NOT NULL,
                    rpm INTEGER NOT NULL,
                    engine_temp_c REAL NOT NULL,
                    fuel_pct REAL NOT NULL
                );
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    client_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    message TEXT NOT NULL
                );
                """
            )
            self._conn.commit()

    def insert_telemetry(self, payload: dict[str, Any]) -> None:
        assert self._conn is not None
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO telemetry_events (
                        received_at, client_id, seq, timestamp,
                        speed_kph, rpm, engine_temp_c, fuel_pct
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["received_at"],
                        payload["client_id"],
                        payload["seq"],
                        payload["timestamp"],
                        payload["speed_kph"],
                        payload["rpm"],
                        payload["engine_temp_c"],
                        payload["fuel_pct"],
                    ),
                )
                self._conn.commit()
            except Exception as exc:  # noqa: BLE001
                raise StorageError(f"Failed to insert telemetry: {exc}") from exc

    def insert_alert(self, payload: dict[str, Any]) -> None:
        assert self._conn is not None
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO alerts (created_at, client_id, seq, code, message)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        payload["created_at"],
                        payload["client_id"],
                        payload["seq"],
                        payload["code"],
                        payload["message"],
                    ),
                )
                self._conn.commit()
            except Exception as exc:  # noqa: BLE001
                raise StorageError(f"Failed to insert alert: {exc}") from exc
