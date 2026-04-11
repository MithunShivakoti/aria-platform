require("dotenv").config();
const dns = require("dns");
dns.setDefaultResultOrder("ipv4first");
const https = require("https");

const key = process.env.OPENAI_API_KEY;

if (!key) {
  console.log("ERROR: OPENAI_API_KEY not found in .env");
  process.exit(1);
}

console.log("Key found:", key.slice(0, 20) + "...");
console.log("Testing...");

const body = JSON.stringify({
  model: "gpt-4o",
  max_tokens: 10,
  messages: [{ role: "user", content: "say ok" }]
});

const req = https.request({
  hostname: "api.openai.com",
  path: "/v1/chat/completions",
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${key}`,
    "Content-Length": Buffer.byteLength(body)
  }
}, res => {
  let data = "";
  res.on("data", chunk => data += chunk);
  res.on("end", () => {
    console.log("HTTP Status:", res.statusCode);
    if (res.statusCode === 200) {
      console.log("SUCCESS: API key is valid and working");
    } else {
      console.log("FAILED:", data);
    }
  });
});

req.on("error", err => console.log("Network error:", err.message));
req.write(body);
req.end();
