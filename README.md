## Socket Telematics

TCP telemetry server + simulated clients.

Key improvements:

- Better information display: server shows an always-visible status line.
- Active clients are continuously visible as `ACTIVE CLIENTS: <N> | ids: ...`.
- Clients can load per-client send intervals from a **text file** (regex parsed) or **XML**.
- SQLite persistence for telemetry + alerts.
- Strong exception handling + safe file handling.

### Run

Start the server:

```bash
python -m socket_telematics server --host 127.0.0.1 --port 5000 --db socket_telematics.db
```

Start clients (in separate terminals):

```bash
python -m socket_telematics client --client-id CAR_101 --interval-config intervals.txt
python -m socket_telematics client --client-id CAR_202 --interval-config intervals.xml
```

### Trigger alerts (fault injection)

Use `--fault` to force telemetry values over alert thresholds:

```bash
python -m socket_telematics client --client-id CAR_HOT --fault overtemp
python -m socket_telematics client --client-id CAR_RACE --fault highrpm
python -m socket_telematics client --client-id CAR_FAST --fault highspeed
python -m socket_telematics client --client-id CAR_EMPTY --fault lowfuel
```

You can repeat `--fault` to trigger multiple alerts from the same client.

If a client id is not present in the interval file, the client uses `--interval`.

### Interval config (text, regex parsed)

See [intervals.txt](intervals.txt).

Allowed examples:

```
CAR_101=1.0
CAR_202:2
CAR_EMPTY = 0.5s
```

Lines starting with `#` or `;` are treated as comments.

### Interval config (XML)

See [intervals.xml](intervals.xml).

Expected shape:

```xml
<clients>
	<client id="CAR_101" interval="1.0" />
</clients>
```

### Persistence

SQLite tables created automatically:

- `telemetry_events`
- `alerts`

Inspect quickly:

```bash
sqlite3 socket_telematics.db ".tables"
sqlite3 socket_telematics.db "select client_id, seq, speed_kph, rpm, engine_temp_c, fuel_pct from telemetry_events order by id desc limit 5;"
```

### Tests

```bash
python -m unittest -v
```

Or with pytest:

```bash
python -m pytest
```
