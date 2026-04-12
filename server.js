// Bypass dotenvx — read .env file directly
const fs = require("fs");
const path = require("path");
try {
  const envFile = fs.readFileSync(path.join(__dirname, ".env"), "utf8");
  envFile.split("\n").forEach(line => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) return;
    const eq = trimmed.indexOf("=");
    if (eq === -1) return;
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim();
    process.env[key] = val;
  });
  console.log("Loaded .env directly. Key ends in:", process.env.OPENAI_API_KEY?.slice(-6));
} catch(e) {
  console.warn("Could not read .env:", e.message);
}

const dns = require("dns");
dns.setServers(["8.8.8.8", "8.8.4.4", "1.1.1.1"]);
dns.setDefaultResultOrder("ipv4first");

const express = require("express");
const https = require("https");
const http = require("http");
const { WebSocket, WebSocketServer } = require("ws");
const { Kafka } = require("kafkajs");
const { createDigitalTwin, loadSensorMetadata } = require("./simulator/digitalTwin");

const app = express();
const PORT = process.env.PORT || 3000;
const DIR = __dirname;
const SENSOR_METADATA_PATH = path.join(DIR, "sensor_metadata.csv");
const LIVE_TOPIC = process.env.KAFKA_SENSOR_TOPIC || "aria.sensor-readings";
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || "127.0.0.1:9092").split(",").map(s => s.trim()).filter(Boolean);
const KAFKA_ENABLED = process.env.KAFKA_ENABLED === "true";
const LIVE_INTERVAL_MS = Number(process.env.LIVE_INTERVAL_MS || 1000);

const sensorMetadata = loadSensorMetadata(SENSOR_METADATA_PATH);
const digitalTwin = createDigitalTwin(sensorMetadata, { scenario: process.env.LIVE_SCENARIO || "normal" });
const wsClients = new Set();
let liveTransport = KAFKA_ENABLED ? "kafka-starting" : "internal";
let liveTimer = null;

console.log("Serving files from:", DIR);
console.log("ARIA.html exists:", fs.existsSync(path.join(DIR, "ARIA.html")));
console.log(`Loaded ${sensorMetadata.length} live sensors from sensor_metadata.csv`);

app.use(express.static(DIR));
app.get("/",     (req, res) => res.sendFile(path.join(DIR, "ARIA.html")));
app.get("/ARIA", (req, res) => res.sendFile(path.join(DIR, "ARIA.html")));

app.get("/api/key", (req, res) => {
  res.json({ configured: !!process.env.OPENAI_API_KEY });
});

app.get("/api/live/status", (req, res) => {
  res.json({
    enabled: true,
    transport: liveTransport,
    kafka_enabled: KAFKA_ENABLED,
    kafka_brokers: KAFKA_ENABLED ? KAFKA_BROKERS : [],
    topic: KAFKA_ENABLED ? LIVE_TOPIC : null,
    scenario: digitalTwin.getScenario(),
    sensor_count: sensorMetadata.length,
    interval_ms: LIVE_INTERVAL_MS,
  });
});

// Proxy to OpenAI API — keeps key server-side, fixes CORS
app.use(express.json({ limit: "4mb" }));

app.post("/api/live/scenario", (req, res) => {
  const scenario = typeof req.body?.scenario === "string" ? req.body.scenario : "normal";
  digitalTwin.setScenario(scenario);
  broadcast({
    type: "live_status",
    transport: liveTransport,
    scenario: digitalTwin.getScenario(),
    sensor_count: sensorMetadata.length,
    timestamp: new Date().toISOString(),
  });
  res.json({ ok: true, scenario: digitalTwin.getScenario(), transport: liveTransport });
});

app.post("/api/chat", (req, res) => {
  const key = process.env.OPENAI_API_KEY;
  if (!key) return res.status(500).json({ error: "API key not configured" });

  const body = JSON.stringify(req.body);

  // Resolve IP first to avoid Windows DNS failures
  dns.resolve4("api.openai.com", (dnsErr, addresses) => {
    const ip = dnsErr ? "api.openai.com" : addresses[0];
    if (dnsErr) console.warn("DNS resolve failed, using hostname:", dnsErr.message);

    const options = {
      hostname: ip,
      path: "/v1/chat/completions",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${key}`,
        "Content-Length": Buffer.byteLength(body),
        "Host": "api.openai.com"
      }
    };

    const apiReq = https.request(options, apiRes => {
      console.log("OpenAI status:", apiRes.statusCode);
      let raw = "";
      apiRes.on("data", chunk => raw += chunk);
      apiRes.on("end", () => {
        if (apiRes.statusCode !== 200) console.log("OpenAI response:", raw.slice(0, 300));
        res.status(apiRes.statusCode).set("Content-Type", "application/json").send(raw);
      });
    });
    apiReq.on("error", err => {
      console.error("OpenAI proxy error:", err.message);
      res.status(502).json({ error: err.message });
    });
    apiReq.write(body);
    apiReq.end();
  });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws/live" });

function broadcast(payload) {
  const data = JSON.stringify(payload);
  for (const ws of wsClients) {
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
  }
}

function startInternalLiveStream(reason = "internal") {
  if (liveTimer) clearInterval(liveTimer);
  liveTransport = reason;
  liveTimer = setInterval(() => {
    const batch = digitalTwin.nextBatch();
    batch.transport = liveTransport;
    broadcast(batch);
  }, LIVE_INTERVAL_MS);
  console.log(`Live stream running via ${liveTransport} transport`);
}

async function startKafkaLiveStream() {
  if (!KAFKA_ENABLED) {
    startInternalLiveStream("internal");
    return;
  }

  try {
    const kafka = new Kafka({
      clientId: "aria-platform",
      brokers: KAFKA_BROKERS,
      connectionTimeout: 2500,
      retry: { retries: 2 },
    });
    const admin = kafka.admin();
    const producer = kafka.producer();
    const consumer = kafka.consumer({ groupId: `aria-dashboard-${Date.now()}` });

    await admin.connect();
    await admin.createTopics({
      waitForLeaders: false,
      topics: [{ topic: LIVE_TOPIC, numPartitions: 1, replicationFactor: 1 }],
    });
    await admin.disconnect();

    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: LIVE_TOPIC, fromBeginning: false });
    liveTransport = "kafka";

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        const payload = JSON.parse(message.value.toString("utf8"));
        payload.transport = "kafka";
        broadcast(payload);
      },
    });

    if (liveTimer) clearInterval(liveTimer);
    liveTimer = setInterval(async () => {
      const batch = digitalTwin.nextBatch();
      await producer.send({
        topic: LIVE_TOPIC,
        messages: [{ key: batch.scenario, value: JSON.stringify(batch) }],
      });
    }, LIVE_INTERVAL_MS);
    console.log(`Kafka live stream connected: ${KAFKA_BROKERS.join(", ")} topic=${LIVE_TOPIC}`);
  } catch (err) {
    console.error("Kafka live stream unavailable, falling back to internal stream:", err.message);
    startInternalLiveStream("internal-fallback");
  }
}

wss.on("connection", ws => {
  wsClients.add(ws);
  ws.send(JSON.stringify({
    type: "live_status",
    transport: liveTransport,
    scenario: digitalTwin.getScenario(),
    sensor_count: sensorMetadata.length,
    timestamp: new Date().toISOString(),
  }));

  ws.on("message", raw => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "set_scenario") {
        digitalTwin.setScenario(msg.scenario || "normal");
        broadcast({
          type: "live_status",
          transport: liveTransport,
          scenario: digitalTwin.getScenario(),
          sensor_count: sensorMetadata.length,
          timestamp: new Date().toISOString(),
        });
      }
    } catch (err) {
      ws.send(JSON.stringify({ type: "live_error", error: err.message }));
    }
  });

  ws.on("close", () => wsClients.delete(ws));
});

server.listen(PORT, () => {
  console.log(`ARIA running at http://localhost:${PORT}/ARIA.html`);
  console.log(`Live WebSocket bridge at ws://localhost:${PORT}/ws/live`);
  startKafkaLiveStream();
});
