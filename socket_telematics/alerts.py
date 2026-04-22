from __future__ import annotations

from dataclasses import dataclass

from .protocol import TelemetryPacket


@dataclass(frozen=True)
class Alert:
    code: str
    message: str


class AlertEngine:
    """Simple rule-based alerts (stdlib-only)."""

    def __init__(
        self,
        *,
        max_speed_kph: float = 100.0,
        max_engine_temp_c: float = 110.0,
        max_rpm: int = 5000,
        min_fuel_pct: float = 10.0,
    ) -> None:
        self._max_speed_kph = max_speed_kph
        self._max_engine_temp_c = max_engine_temp_c
        self._max_rpm = max_rpm
        self._min_fuel_pct = min_fuel_pct

    def evaluate(self, pkt: TelemetryPacket) -> list[Alert]:
        alerts: list[Alert] = []

        if pkt.speed_kph > self._max_speed_kph:
            alerts.append(Alert(code="SPEED_HIGH", message=f"Vehicle speed high: {pkt.speed_kph:.1f} kph"))

        if pkt.engine_temp_c >= self._max_engine_temp_c:
            alerts.append(Alert(code="ENGINE_OVER_TEMP", message=f"Engine temperature high: {pkt.engine_temp_c:.1f}C"))

        if pkt.rpm >= self._max_rpm:
            alerts.append(Alert(code="RPM_HIGH", message=f"Engine RPM high: {pkt.rpm}"))

        if pkt.fuel_pct <= self._min_fuel_pct:
            alerts.append(Alert(code="FUEL_LOW", message=f"Fuel low: {pkt.fuel_pct:.1f}%"))

        return alerts
