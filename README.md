# ARIA — Autonomous Risk Intelligence Agent

A real-time offshore oil & gas platform monitoring dashboard for **North Sea Platform Alpha**. Features anomaly detection, multi-sensor correlation, AI-powered work order generation, and live Kafka streaming.

---

## Prerequisites

- [Node.js](https://nodejs.org/) v18+
- [Python](https://www.python.org/) 3.10+
- [Java](https://www.java.com/) 11+ (for Kafka)
- An OpenAI API key

---

## Setup

### 1. Install Node dependencies
```bash
npm install
```

### 2. Install Python dependencies
```bash
pip install kafka-python websockets pandas
```

### 3. Configure your API key
Create a `.env` file in the project root:
```
OPENAI_API_KEY=your-openai-api-key-here
PORT=3000
```

Optional critical alert email notifications:
```
ALERT_EMAIL_TO=madumita240912@gmail.com
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-gmail-address@gmail.com
SMTP_PASS=your-gmail-app-password
SMTP_FROM=ARIA Alerts <your-gmail-address@gmail.com>
ALERT_EMAIL_COOLDOWN_MS=300000
```
For Gmail, use an app password for `SMTP_PASS`; do not use your normal account password. `ALERT_EMAIL_COOLDOWN_MS` prevents repeat emails for the same critical alert ID for 5 minutes by default.

### 4. Download Kafka (first time only)
```bash
curl -L -o kafka.tgz "https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz"
tar -xzf kafka.tgz
rm kafka.tgz
```
Then update the log directory in `kafka_2.13-3.7.0/config/kraft/server.properties`:
```
log.dirs=C:/kafka-logs
```

---

## Running the Dashboard (Replay Mode)

No Kafka needed — uses pre-computed Z-score data.

**Terminal 1:**
```powershell
node server.js
```

Open browser at `http://localhost:3000/ARIA.html`

---

## Running with Live Kafka Streaming

Requires 4 terminals running simultaneously.

### Step 1 — Start Kafka (first time: format storage)
```powershell
# Map Kafka to short drive (avoids Windows path length limit)
subst K: ".\kafka_2.13-3.7.0"

# Generate UUID and format storage (first time only)
K:\bin\windows\kafka-storage.bat random-uuid
K:\bin\windows\kafka-storage.bat format -t <YOUR-UUID> -c K:\config\kraft\server.properties

# Start broker (keep this window open)
K:\bin\windows\kafka-server-start.bat K:\config\kraft\server.properties
```
Wait for: `Kafka Server started`

Then create the topic (once):
```powershell
K:\bin\windows\kafka-topics.bat --create --topic sensor-readings --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --if-not-exists
```

### Step 2 — Start Kafka (subsequent runs)
```powershell
subst K: ".\kafka_2.13-3.7.0"
K:\bin\windows\kafka-server-start.bat K:\config\kraft\server.properties
```

### Step 3 — Start Producer (new terminal)
```powershell
python kafka_producer.py
```
Streams `timeseries.csv` row-by-row to the `sensor-readings` Kafka topic at 0.5s intervals.

### Step 4 — Start WebSocket Bridge (new terminal)
```powershell
python kafka_bridge.py
```
Consumes Kafka messages and broadcasts to the browser via WebSocket on `ws://localhost:8765`.

### Step 5 — Start Node Server (new terminal)
```powershell
node server.js
```

### Step 6 — Open Dashboard
```
http://localhost:3000/ARIA_v2.html
```
Click **📡 GO LIVE** — the button turns red and sensor values update in real time from Kafka.

---

## Project Structure

```
├── ARIA.html               # Main dashboard (replay mode)
├── ARIA_v2.html            # Dashboard with live Kafka streaming
├── server.js               # Node.js Express server + OpenAI proxy
├── kafka_producer.py       # Reads timeseries.csv, sends to Kafka
├── kafka_bridge.py         # Kafka → WebSocket bridge for browser
├── start_kafka.bat         # Windows batch script to start Kafka
├── start_all.sh            # Unix script to start everything
├── zscore_results.json     # Pre-computed Z-scores per sensor
├── pitch_numbers.json      # Predictive lead time per sensor
├── rag_documents.json      # SOP documents for AI context
├── all_assets.json         # Full asset registry
├── all_sensors.json        # Full sensor registry
├── maintenance_history.json# Historical maintenance records
├── .env                    # API key (not committed)
└── kafka_2.13-3.7.0/      # Kafka installation (not committed)
```

---

## Architecture

```
timeseries.csv
      ↓
kafka_producer.py  →  Kafka Topic: sensor-readings
                              ↓
                       kafka_bridge.py  →  WebSocket ws://localhost:8765
                                                    ↓
                                             ARIA_v2.html
                                          (📡 GO LIVE mode)

ARIA.html  ←→  server.js  ←→  OpenAI API (gpt-4o)
```

---

## Features

- **Real-time anomaly detection** — Z-score based alerts across 50+ sensors
- **Sparkline charts** — Z-score trend per sensor with predictive alert markers
- **Multi-sensor correlation** — HIGH/MEDIUM/LOW confidence scoring per asset
- **Asset failure history** — pattern matching against historical events
- **AI chat (ARIA)** — GPT-4o with full SOP context and asset history
- **Work order generation** — auto-incremented WOs with SOP sections and download
- **Live Kafka streaming** — real-time sensor values via Kafka → WebSocket

---

## Notes

- `timeseries.csv`, `.env`, and `kafka_2.13-3.7.0/` are gitignored
- The OpenAI API key never leaves the server — the browser only calls `/api/chat` on localhost
- On Windows, if you get `ENOTFOUND api.openai.com`, the server automatically retries using Google DNS (8.8.8.8)
