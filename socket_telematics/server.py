from __future__ import annotations

import logging
import logging.handlers
import signal
import socket
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .alerts import AlertEngine
from .exceptions import ProtocolError, StorageError
from .protocol import TelemetryPacket, encode_ndjson, iter_ndjson_from_buffer, make_ack, make_alert, make_error
from .status import StatusLine
from .storage import SQLiteStorage, StorageConfig


def _configure_logging(level: str) -> logging.Logger:
    logger = logging.getLogger("socket_telematics")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(threadName)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    if not logger.handlers:
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    return logger


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 5000
    db_path: str = "socket_telematics.db"
    log_level: str = "INFO"
    status_refresh_s: float = 1.0


class ClientHandler(threading.Thread):
    def __init__(
        self,
        *,
        conn: socket.socket,
        addr: tuple[str, int],
        shutdown: threading.Event,
        storage: SQLiteStorage,
        alerts: AlertEngine,
        logger: logging.Logger,
        update_identity: Callable[["ClientHandler", str], None],
        on_disconnect: Callable[["ClientHandler"], None],
    ) -> None:
        super().__init__(daemon=True, name=f"client-{addr[0]}:{addr[1]}")
        self._conn = conn
        self._addr = addr
        self._shutdown = shutdown
        self._storage = storage
        self._alerts = alerts
        self._log = logger
        self._update_identity = update_identity
        self._on_disconnect = on_disconnect
        self._buffer = bytearray()
        self._send_lock = threading.Lock()
        self.client_id: str | None = None

    def run(self) -> None:
        self._conn.settimeout(1.0)
        self._log.info("client_connected addr=%s:%s", self._addr[0], self._addr[1])

        try:
            while not self._shutdown.is_set():
                try:
                    chunk = self._conn.recv(4096)
                except socket.timeout:
                    continue

                if not chunk:
                    break

                self._buffer.extend(chunk)
                try:
                    messages, self._buffer = iter_ndjson_from_buffer(self._buffer)
                except ProtocolError as exc:
                    self._send(make_error("UNKNOWN", None, code="BAD_JSON", message=str(exc)))
                    continue

                for msg in messages:
                    self._handle_message(msg)

        except Exception:  # noqa: BLE001
            self._log.exception("client_handler_error addr=%s:%s", self._addr[0], self._addr[1])
        finally:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._on_disconnect(self)
            self._log.info("client_disconnected addr=%s:%s client_id=%s", self._addr[0], self._addr[1], self.client_id or "?")

    def _handle_message(self, msg: dict[str, Any]) -> None:
        msg_type = msg.get("type")
        client_id = msg.get("client_id") if isinstance(msg.get("client_id"), str) else "UNKNOWN"
        seq = msg.get("seq") if isinstance(msg.get("seq"), int) else None

        if msg_type != "telemetry":
            self._send(make_error(client_id, seq, code="BAD_MESSAGE", message="Unknown message type"))
            return

        try:
            pkt = TelemetryPacket.from_dict(msg)
        except ProtocolError as exc:
            self._send(make_error(client_id, seq, code="BAD_MESSAGE", message=str(exc)))
            return

        if self.client_id != pkt.client_id:
            self.client_id = pkt.client_id
            self._update_identity(self, pkt.client_id)

        received_at = _utc_now_iso()
        try:
            self._storage.insert_telemetry(
                {
                    "received_at": received_at,
                    "client_id": pkt.client_id,
                    "seq": pkt.seq,
                    "timestamp": pkt.timestamp,
                    "speed_kph": pkt.speed_kph,
                    "rpm": pkt.rpm,
                    "engine_temp_c": pkt.engine_temp_c,
                    "fuel_pct": pkt.fuel_pct,
                }
            )
        except StorageError:
            self._log.exception("storage_insert_failed client_id=%s seq=%s", pkt.client_id, pkt.seq)

        self._log.info(
            "telemetry client=%s seq=%s speed=%.1f rpm=%s temp=%.1f fuel=%.1f",
            pkt.client_id,
            pkt.seq,
            pkt.speed_kph,
            pkt.rpm,
            pkt.engine_temp_c,
            pkt.fuel_pct,
        )

        self._send(make_ack(pkt.client_id, pkt.seq))

        for alert in self._alerts.evaluate(pkt):
            alert_msg = make_alert(pkt.client_id, pkt.seq, code=alert.code, message=alert.message)
            try:
                self._storage.insert_alert(
                    {
                        "created_at": _utc_now_iso(),
                        "client_id": pkt.client_id,
                        "seq": pkt.seq,
                        "code": alert.code,
                        "message": alert.message,
                    }
                )
            except StorageError:
                self._log.exception("storage_alert_insert_failed client_id=%s seq=%s", pkt.client_id, pkt.seq)

            self._send(alert_msg)

    def _send(self, message: dict[str, Any]) -> None:
        data = encode_ndjson(message)
        with self._send_lock:
            try:
                self._conn.sendall(data)
            except Exception:  # noqa: BLE001
                pass


class TelemetryServer:
    def __init__(self, config: ServerConfig) -> None:
        self._config = config
        self._shutdown = threading.Event()
        self._log = _configure_logging(config.log_level)
        self._storage = SQLiteStorage(StorageConfig(db_path=config.db_path))
        self._alerts = AlertEngine()

        self._clients_lock = threading.Lock()
        self._clients: set[ClientHandler] = set()

        self._status: StatusLine | None = None

    def _status_text(self) -> str:
        with self._clients_lock:
            client_ids = sorted({c.client_id for c in self._clients if c.client_id})
            active = len(self._clients)

        ids = ",".join(client_ids) if client_ids else "-"
        return f"ACTIVE CLIENTS: {active} | ids: {ids}"

    def _update_identity(self, _handler: ClientHandler, _client_id: str) -> None:
        # No-op: status thread reads handler.client_id.
        return

    def _on_disconnect(self, handler: ClientHandler) -> None:
        with self._clients_lock:
            self._clients.discard(handler)

    def serve_forever(self) -> int:
        self._storage.start()

        self._status = StatusLine(get_status=self._status_text, shutdown=self._shutdown, refresh_s=self._config.status_refresh_s)
        self._status.start()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self._config.host, self._config.port))
            s.listen()
            s.settimeout(1.0)

            self._log.info("server_started host=%s port=%s db=%s", self._config.host, self._config.port, self._config.db_path)

            while not self._shutdown.is_set():
                try:
                    conn, addr = s.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                handler = ClientHandler(
                    conn=conn,
                    addr=addr,
                    shutdown=self._shutdown,
                    storage=self._storage,
                    alerts=self._alerts,
                    logger=self._log,
                    update_identity=self._update_identity,
                    on_disconnect=self._on_disconnect,
                )
                with self._clients_lock:
                    self._clients.add(handler)
                handler.start()

        self._log.info("server_stopping")
        self._shutdown.set()
        with self._clients_lock:
            clients = list(self._clients)
        for t in clients:
            t.join(timeout=2.0)
        self._storage.stop()
        self._log.info("server_stopped")
        return 0

    def shutdown(self) -> None:
        self._shutdown.set()


def install_signal_handlers(server: TelemetryServer) -> None:
    def _handler(_signum: int, _frame) -> None:  # type: ignore[no-untyped-def]
        server.shutdown()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def run_server(*, host: str, port: int, db_path: str, log_level: str, status_refresh_s: float) -> int:
    server = TelemetryServer(
        ServerConfig(host=host, port=port, db_path=db_path, log_level=log_level, status_refresh_s=status_refresh_s)
    )
    install_signal_handlers(server)
    return server.serve_forever()
