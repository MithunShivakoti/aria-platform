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
    if (process.env[key] !== undefined) return;
    process.env[key] = val;
  });
  console.log("Loaded .env directly.");
} catch(e) {
  console.warn("Could not read .env:", e.message);
}

const dns = require("dns");
dns.setServers(["8.8.8.8", "8.8.4.4", "1.1.1.1"]);
dns.setDefaultResultOrder("ipv4first");

const express = require("express");
const https = require("https");
const http = require("http");
const nodemailer = require("nodemailer");
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
const ALERT_EMAIL_TO = process.env.ALERT_EMAIL_TO || "madumita240912@gmail.com";

const sensorMetadata = loadSensorMetadata(SENSOR_METADATA_PATH);
const digitalTwin = createDigitalTwin(sensorMetadata, { scenario: process.env.LIVE_SCENARIO || "normal" });
const wsClients = new Set();
const emailedCriticalAlertIds = new Map();
const ALERT_EMAIL_COOLDOWN_MS = Number(process.env.ALERT_EMAIL_COOLDOWN_MS || 5 * 60 * 1000);
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

app.get("/api/health", (req, res) => {
  res.json({
    ok: true,
    service: "aria-platform",
    live_transport: liveTransport,
    kafka_enabled: KAFKA_ENABLED,
    sensor_count: sensorMetadata.length,
    uptime_seconds: Math.round(process.uptime()),
  });
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

function getAlertEmailTransporter() {
  const host = process.env.SMTP_HOST;
  const port = Number(process.env.SMTP_PORT || 587);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  if (!host || !user || !pass) return null;
  return nodemailer.createTransport({
    host,
    port,
    secure: port === 465,
    auth: { user, pass },
  });
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, char => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[char]));
}

function formatCriticalAlertEmail(alert, asset) {
  const safeAlert = alert || {};
  const safeAsset = asset || {};
  const lines = [
    `Severity: ${safeAlert.severity || "CRITICAL"}`,
    `Asset: ${safeAsset.tag || "Unknown"}${safeAsset.name ? ` - ${safeAsset.name}` : ""}`,
    `Sensor: ${safeAlert.sensor || "Unknown"} = ${safeAlert.sensorVal ?? "--"} ${safeAlert.sensorUnit || ""}`,
    `Time: ${safeAlert.timestamp || new Date().toISOString()}`,
    `Title: ${safeAlert.title || "Critical ARIA alert"}`,
    `Description: ${safeAlert.desc || "No description provided."}`,
    `Failure mode: ${safeAlert.failureMode || "Not specified"}`,
    `SOP: ${safeAlert.sop || "Not specified"}`,
  ];
  const htmlRows = lines.map(line => {
    const [label, ...rest] = line.split(":");
    return `<tr><td style="padding:6px 10px;color:#64748b;font-weight:700">${escapeHtml(label)}</td><td style="padding:6px 10px">${escapeHtml(rest.join(":").trim())}</td></tr>`;
  }).join("");
  return {
    text: `ARIA Critical Alert\n\n${lines.join("\n")}`,
    html: `
      <div style="font-family:Arial,sans-serif;color:#0f172a">
        <h2 style="color:#c92846;margin:0 0 12px">ARIA Critical Alert</h2>
        <table style="border-collapse:collapse;border:1px solid #d8e1ea">${htmlRows}</table>
        <p style="margin-top:14px;color:#475569">Review the dashboard and generate the required work order if this alert is still active.</p>
      </div>
    `,
  };
}

app.post("/api/alerts/email", async (req, res) => {
  const alert = req.body?.alert || {};
  const asset = req.body?.asset || {};
  if (alert.severity !== "CRITICAL") {
    return res.status(400).json({ ok: false, error: "Only CRITICAL alerts trigger email." });
  }

  const alertId = alert.id || `${alert.sensor || "unknown"}-${alert.timestamp || Date.now()}`;
  const lastSentAt = emailedCriticalAlertIds.get(alertId) || 0;
  if (Date.now() - lastSentAt < ALERT_EMAIL_COOLDOWN_MS) {
    return res.json({ ok: true, skipped: true, reason: "cooldown" });
  }

  const transporter = getAlertEmailTransporter();
  if (!transporter) {
    console.warn("Critical alert email not sent: SMTP_HOST, SMTP_USER, or SMTP_PASS is missing.");
    return res.status(503).json({ ok: false, error: "Email is not configured. Set SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, and SMTP_FROM in .env." });
  }

  const subjectTag = asset.tag || alert.sensor || "ARIA";
  const content = formatCriticalAlertEmail(alert, asset);
  try {
    await transporter.sendMail({
      from: process.env.SMTP_FROM || process.env.SMTP_USER,
      to: ALERT_EMAIL_TO,
      subject: `[ARIA CRITICAL] ${subjectTag} - ${alert.title || "Critical alert"}`,
      text: content.text,
      html: content.html,
    });
    emailedCriticalAlertIds.set(alertId, Date.now());
    console.log(`Critical alert email sent to ${ALERT_EMAIL_TO}: ${alertId}`);
    res.json({ ok: true, to: ALERT_EMAIL_TO });
  } catch (err) {
    console.error("Critical alert email failed:", err.message);
    res.status(502).json({ ok: false, error: err.message });
  }
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

    // Consumer only — kafka_producer.py is the data source
    const consumer = kafka.consumer({ groupId: `aria-dashboard-${Date.now()}` });

    await consumer.connect();
    await consumer.subscribe({ topic: LIVE_TOPIC, fromBeginning: false });
    liveTransport = "kafka";

    await consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        try {
          const payload = JSON.parse(message.value.toString("utf8"));
          payload.transport = "kafka";
          broadcast(payload);
        } catch (parseErr) {
          console.warn("Kafka message parse error:", parseErr.message);
        }
      },
    });

    console.log(`Kafka consumer ready: ${KAFKA_BROKERS.join(", ")} topic=${LIVE_TOPIC}`);
    console.log(`Waiting for data from kafka_producer.py...`);
  } catch (err) {
    console.error("Kafka unavailable, falling back to internal stream:", err.message);
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
