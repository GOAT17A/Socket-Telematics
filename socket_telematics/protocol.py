from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .exceptions import ProtocolError


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def encode_ndjson(message: dict[str, Any]) -> bytes:
    return (json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


def iter_ndjson_from_buffer(buffer: bytearray) -> tuple[list[dict[str, Any]], bytearray]:
    messages: list[dict[str, Any]] = []

    while True:
        newline_index = buffer.find(b"\n")
        if newline_index < 0:
            break

        line = bytes(buffer[:newline_index])
        del buffer[: newline_index + 1]

        if not line.strip():
            continue

        try:
            obj = json.loads(line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProtocolError(f"Invalid JSON line: {exc}") from exc

        if not isinstance(obj, dict):
            raise ProtocolError("Message must be a JSON object")

        messages.append(obj)

    return messages, buffer


def require_str(obj: dict[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise ProtocolError(f"Missing/invalid '{key}'")
    return value


def require_int(obj: dict[str, Any], key: str) -> int:
    value = obj.get(key)
    if not isinstance(value, int):
        raise ProtocolError(f"Missing/invalid '{key}'")
    return value


def require_number(obj: dict[str, Any], key: str) -> float:
    value = obj.get(key)
    if not isinstance(value, (int, float)):
        raise ProtocolError(f"Missing/invalid '{key}'")
    return float(value)


def validate_timestamp_iso(ts: str) -> None:
    try:
        datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception as exc:  # noqa: BLE001
        raise ProtocolError("Invalid 'timestamp' (expected ISO-8601)") from exc


@dataclass(frozen=True)
class TelemetryPacket:
    client_id: str
    seq: int
    timestamp: str
    speed_kph: float
    rpm: int
    engine_temp_c: float
    fuel_pct: float

    @staticmethod
    def from_dict(obj: dict[str, Any]) -> "TelemetryPacket":
        msg_type = require_str(obj, "type")
        if msg_type != "telemetry":
            raise ProtocolError("Not a telemetry message")

        client_id = require_str(obj, "client_id")
        seq = require_int(obj, "seq")
        timestamp = require_str(obj, "timestamp")
        validate_timestamp_iso(timestamp)

        speed_kph = require_number(obj, "speed_kph")
        rpm = require_int(obj, "rpm")
        engine_temp_c = require_number(obj, "engine_temp_c")
        fuel_pct = require_number(obj, "fuel_pct")

        if rpm < 0:
            raise ProtocolError("'rpm' must be >= 0")
        if not (0.0 <= fuel_pct <= 100.0):
            raise ProtocolError("'fuel_pct' must be 0..100")

        return TelemetryPacket(
            client_id=client_id,
            seq=seq,
            timestamp=timestamp,
            speed_kph=float(speed_kph),
            rpm=int(rpm),
            engine_temp_c=float(engine_temp_c),
            fuel_pct=float(fuel_pct),
        )


def make_ack(client_id: str, seq: int, message: str = "ok") -> dict[str, Any]:
    return {
        "type": "ack",
        "timestamp": utc_now_iso(),
        "client_id": client_id,
        "seq": seq,
        "message": message,
    }


def make_error(client_id: str, seq: int | None, code: str, message: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "type": "error",
        "timestamp": utc_now_iso(),
        "client_id": client_id,
        "code": code,
        "message": message,
    }
    if seq is not None:
        payload["seq"] = seq
    return payload


def make_alert(client_id: str, seq: int, code: str, message: str) -> dict[str, Any]:
    return {
        "type": "alert",
        "timestamp": utc_now_iso(),
        "client_id": client_id,
        "seq": seq,
        "code": code,
        "message": message,
    }
