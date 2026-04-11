require("dotenv").config();
const dns = require("dns");
dns.setDefaultResultOrder("ipv4first");
const express = require("express");
const path = require("path");
const fs = require("fs");
const https = require("https");

const app = express();
const PORT = process.env.PORT || 3000;
const DIR = __dirname;

console.log("Serving files from:", DIR);
console.log("ARIA.html exists:", fs.existsSync(path.join(DIR, "ARIA.html")));

// Serve all static files
app.use(express.static(DIR));

// Explicit routes
app.get("/",      (req, res) => res.sendFile(path.join(DIR, "ARIA.html")));
app.get("/ARIA",  (req, res) => res.sendFile(path.join(DIR, "ARIA.html")));

// Tell the frontend whether a key is configured — never send the actual key
app.get("/api/key", (req, res) => {
  res.json({ configured: !!process.env.OPENAI_API_KEY });
});

// Proxy to OpenAI API — keeps key server-side, fixes CORS
app.use(express.json({ limit: "4mb" }));
app.post("/api/chat", (req, res) => {
  const key = process.env.OPENAI_API_KEY;
  if (!key) return res.status(500).json({ error: "API key not configured" });

  const body = JSON.stringify(req.body);
  const options = {
    hostname: "api.openai.com",
    path: "/v1/chat/completions",
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${key}`,
      "Content-Length": Buffer.byteLength(body)
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
    console.error("OpenAI proxy error:", err);
    res.status(502).json({ error: err.message });
  });
  apiReq.write(body);
  apiReq.end();
});

app.listen(PORT, () => {
  console.log(`ARIA running at http://localhost:${PORT}/ARIA.html`);
});
