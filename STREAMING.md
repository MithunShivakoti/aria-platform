# ARIA Live Digital Twin Stream

ARIA now has two data modes:

- Historical Replay: existing six-month replay/demo timeline.
- Live Digital Twin: synthetic real-time readings for all 175 sensors, calibrated from `sensor_metadata.csv` thresholds and routed through the Node WebSocket bridge.

## Demo-safe local mode

This works without Kafka:

```bash
npm start
```

Open `http://127.0.0.1:3000/ARIA.html`, select `Live Digital Twin`, then choose a scenario.

## Kafka/Redpanda mode

Start a Kafka-compatible broker:

```bash
docker compose -f docker-compose.kafka.yml up
```

In another terminal:

```bash
KAFKA_ENABLED=true KAFKA_BROKERS=127.0.0.1:9092 npm start
```

The server produces digital-twin batches to topic `aria.sensor-readings`, consumes them back through Kafka, and broadcasts them to the browser over `ws://localhost:3000/ws/live`.

## Scenarios

- `normal`
- `p101_bearing`
- `k201_surge`
- `e301_fouling`
- `pt145_drift`
- `p501_export_leak`

The browser sends scenario changes to the Node bridge; the same digital twin drives the internal fallback and Kafka-backed mode.
