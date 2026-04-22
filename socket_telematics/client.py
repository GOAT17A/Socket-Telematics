from __future__ import annotations

import random
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .exceptions import ConfigError, ProtocolError
from .intervals import load_intervals
from .protocol import encode_ndjson, iter_ndjson_from_buffer, make_error


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class ClientConfig:
    host: str
    port: int
    client_id: str
    default_interval_s: float
    interval_config_path: str | None
    faults: frozenset[str]


class TelemetryClient:
    def __init__(self, config: ClientConfig) -> None:
        self._config = config
        self._seq = 0

        self._interval_s = config.default_interval_s
        if config.interval_config_path:
            interval_cfg = load_intervals(Path(config.interval_config_path))
            self._interval_s = interval_cfg.interval_for(config.client_id, config.default_interval_s)

    def run(self) -> None:
        if self._interval_s <= 0:
            raise ConfigError("Interval must be > 0")
        buffer = bytearray()

        with socket.create_connection((self._config.host, self._config.port), timeout=5.0) as sock:
            sock.settimeout(1.0)
            while True:
                self._send_one(sock)
                should_continue = self._drain_responses(sock, buffer, window_s=0.2)
                if not should_continue:
                    return
                time.sleep(self._interval_s)

    def _send_one(self, sock: socket.socket) -> None:
        self._seq += 1
        msg = self._make_telemetry(self._seq)
        try:
            sock.sendall(encode_ndjson(msg))
        except OSError as exc:
            raise ProtocolError(f"Failed to send telemetry: {exc}") from exc

    def _drain_responses(self, sock: socket.socket, buffer: bytearray, *, window_s: float) -> bool:
        """Drain server responses for a short time window.

        Returns False if the socket closes.
        """
        start = time.time()
        while time.time() - start < window_s:
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                break

            if not chunk:
                return False

            buffer.extend(chunk)
            try:
                messages, remaining = iter_ndjson_from_buffer(buffer)
            except ProtocolError as exc:
                # If we somehow get invalid server JSON, surface it but continue.
                print(
                    encode_ndjson(
                        make_error(self._config.client_id, self._seq, "BAD_SERVER_JSON", str(exc))
                    ).decode("utf-8").strip()
                )
                buffer.clear()
                break

            buffer[:] = remaining
            for m in messages:
                print(m)

        return True

    def _make_telemetry(self, seq: int) -> dict[str, Any]:
        # Default telemetry is mild; faults can push values over alert thresholds.
        speed = 20.0 + random.random() * 80.0
        rpm = int(700 + random.random() * 2500)
        temp = 75.0 + random.random() * 30.0
        fuel = max(0.0, 100.0 - seq * 0.2)

        if "highrpm" in self._config.faults:
            rpm = max(rpm, 5200)
        if "highspeed" in self._config.faults:
            speed = max(speed, 105.0)
        if "overtemp" in self._config.faults:
            temp = max(temp, 115.0)
        if "lowfuel" in self._config.faults:
            fuel = min(fuel, 5.0)

        return {
            "type": "telemetry",
            "timestamp": _utc_now_iso(),
            "client_id": self._config.client_id,
            "seq": seq,
            "speed_kph": round(speed, 1),
            "rpm": rpm,
            "engine_temp_c": round(temp, 1),
            "fuel_pct": round(fuel, 1),
        }


def run_client(
    *,
    host: str,
    port: int,
    client_id: str,
    interval_s: float,
    interval_config: str | None,
    faults: set[str] | frozenset[str] | None = None,
) -> int:
    client = TelemetryClient(
        ClientConfig(
            host=host,
            port=port,
            client_id=client_id,
            default_interval_s=interval_s,
            interval_config_path=interval_config,
            faults=frozenset(faults or ()),
        )
    )
    client.run()
    return 0
