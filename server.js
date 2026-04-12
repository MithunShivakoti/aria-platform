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

const app = express();
const PORT = process.env.PORT || 3000;
const DIR = __dirname;

console.log("Serving files from:", DIR);
console.log("ARIA.html exists:", fs.existsSync(path.join(DIR, "ARIA.html")));

app.use(express.static(DIR));
app.get("/",     (req, res) => res.sendFile(path.join(DIR, "ARIA.html")));
app.get("/ARIA", (req, res) => res.sendFile(path.join(DIR, "ARIA.html")));

app.get("/api/key", (req, res) => {
  res.json({ configured: !!process.env.OPENAI_API_KEY });
});

app.use(express.json({ limit: "4mb" }));
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

app.listen(PORT, () => {
  console.log(`ARIA running at http://localhost:${PORT}/ARIA.html`);
});
