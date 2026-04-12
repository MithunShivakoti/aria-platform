const fs = require("fs");

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let quoted = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"' && line[i + 1] === '"') {
      current += '"';
      i++;
    } else if (ch === '"') {
      quoted = !quoted;
    } else if (ch === "," && !quoted) {
      values.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  values.push(current);
  return values;
}

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function loadSensorMetadata(filePath) {
  const text = fs.readFileSync(filePath, "utf8").trim();
  const [headerLine, ...lines] = text.split(/\r?\n/);
  const headers = parseCsvLine(headerLine);
  return lines.filter(Boolean).map(line => {
    const row = parseCsvLine(line);
    const sensor = Object.fromEntries(headers.map((key, i) => [key, row[i] ?? ""]));
    return {
      sensor_id: sensor.sensor_id,
      asset_id: sensor.asset_id,
      tag: sensor.tag,
      name: sensor.name,
      sensor_type: sensor.sensor_type,
      unit: sensor.unit,
      normal_min: toNumber(sensor.normal_min),
      normal_max: toNumber(sensor.normal_max),
      alarm_low: toNumber(sensor.alarm_low),
      alarm_high: toNumber(sensor.alarm_high),
      trip_low: toNumber(sensor.trip_low),
      trip_high: toNumber(sensor.trip_high),
      area: sensor.area,
      location: sensor.location,
    };
  });
}

function hashString(input) {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return Math.abs(hash >>> 0);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function classify(value, sensor) {
  if (sensor.trip_high !== null && value >= sensor.trip_high) return "TRIP_HIGH";
  if (sensor.trip_low !== null && value <= sensor.trip_low) return "TRIP_LOW";
  if (sensor.alarm_high !== null && value >= sensor.alarm_high) return "ALARM_HIGH";
  if (sensor.alarm_low !== null && value <= sensor.alarm_low) return "ALARM_LOW";
  if (sensor.normal_max !== null && value > sensor.normal_max) return "WARN_HIGH";
  if (sensor.normal_min !== null && value < sensor.normal_min) return "WARN_LOW";
  return "NORMAL";
}

function severityForState(state) {
  if (state.startsWith("TRIP")) return "CRITICAL";
  if (state.startsWith("ALARM")) return "HIGH";
  if (state.startsWith("WARN")) return "MEDIUM";
  return "NORMAL";
}

function createDigitalTwin(sensorMetadata, options = {}) {
  const startedAt = options.startedAt || new Date();
  let tick = 0;
  const previous = new Map();
  let activeScenario = options.scenario || "normal";

  function setScenario(scenario) {
    activeScenario = scenario || "normal";
    tick = 0;
    previous.clear();
  }

  function scenarioLoad(sensor, elapsedSeconds) {
    const ramp = clamp((elapsedSeconds - 8) / 90, 0, 1);
    const fastRamp = clamp((elapsedSeconds - 4) / 35, 0, 1);
    const span = Math.max(1, sensor.normal_max - sensor.normal_min);
    const highAlarm = sensor.alarm_high ?? (sensor.normal_max + span * 0.15);
    const lowAlarm = sensor.alarm_low ?? (sensor.normal_min - span * 0.15);

    if (activeScenario === "p101_bearing") {
      if (sensor.sensor_id === "P-101-VIB") return ramp * (highAlarm - sensor.normal_max + span * 0.42);
      if (sensor.sensor_id === "P-101-TEMP") return ramp * span * 0.42;
      if (sensor.sensor_id === "P-101-CURR") return ramp * span * 0.32;
      if (sensor.sensor_id === "P-101-FLOW") return -ramp * span * 0.55;
    }

    if (activeScenario === "k201_surge") {
      if (sensor.sensor_id === "V-201-LEVEL") return fastRamp * (highAlarm - sensor.normal_max + span * 0.35);
      if (sensor.sensor_id === "K-201-VIBDE" || sensor.sensor_id === "K-201-VIBNDE") return fastRamp * span * 0.82;
      if (sensor.sensor_id === "K-201-DTEMP") return fastRamp * span * 0.48;
      if (sensor.sensor_id === "K-201-FLOW") return -fastRamp * span * 0.44;
    }

    if (activeScenario === "e301_fouling") {
      if (sensor.sensor_id === "E-301-TOUTLET" || sensor.sensor_id === "TT-301-PV" || sensor.sensor_id === "TT-302-PV") return ramp * span * 0.68;
      if (sensor.sensor_id === "E-301-FLOW") return -ramp * span * 0.18;
      if (sensor.sensor_id === "E-301-PSHELL") return ramp * span * 0.18;
    }

    if (activeScenario === "pt145_drift") {
      if (sensor.sensor_id === "PT-145-PV") return ramp * span * 0.72;
      if (sensor.sensor_id === "PT-146-PV") return 0;
    }

    if (activeScenario === "p501_export_leak") {
      if (sensor.sensor_id === "P-501-PRESS" || sensor.sensor_id === "PT-501-PV" || sensor.sensor_id === "PT-502-PV") return -fastRamp * (sensor.normal_min - lowAlarm + span * 0.35);
      if (sensor.sensor_id === "P-501-FLOW" || sensor.sensor_id === "FT-501-PV") return -fastRamp * span * 0.65;
      if (sensor.sensor_id === "P-501-CURR") return fastRamp * span * 0.28;
    }

    return 0;
  }

  function generateReading(sensor, timestamp, elapsedSeconds) {
    const mid = (sensor.normal_min + sensor.normal_max) / 2;
    const span = Math.max(1, sensor.normal_max - sensor.normal_min);
    const hash = hashString(sensor.sensor_id);
    const phase = (hash % 360) * Math.PI / 180;
    const slowCycle = Math.sin(elapsedSeconds / 41 + phase) * span * 0.035;
    const processCycle = Math.sin(elapsedSeconds / 9 + phase * 0.37) * span * 0.012;
    const noise = (Math.random() - 0.5) * span * 0.018;
    const target = mid + slowCycle + processCycle + noise + scenarioLoad(sensor, elapsedSeconds);
    const last = previous.has(sensor.sensor_id) ? previous.get(sensor.sensor_id) : target;
    const value = last * 0.72 + target * 0.28;
    previous.set(sensor.sensor_id, value);

    const state = classify(value, sensor);
    const severity = severityForState(state);
    const qualityRoll = Math.random();
    const qualityFlag = qualityRoll < 0.002 ? "BAD" : qualityRoll < 0.012 ? "UNCERTAIN" : "GOOD";

    return {
      timestamp,
      sensor_id: sensor.sensor_id,
      asset_id: sensor.asset_id,
      tag: sensor.tag,
      name: sensor.name,
      sensor_type: sensor.sensor_type,
      value: Number(value.toFixed(3)),
      unit: sensor.unit,
      quality_flag: qualityFlag,
      state,
      severity,
      scenario_id: activeScenario,
      source: "ARIA_DIGITAL_TWIN",
    };
  }

  function nextBatch() {
    const elapsedSeconds = tick++;
    const timestamp = new Date(startedAt.getTime() + elapsedSeconds * 1000).toISOString();
    const readings = sensorMetadata.map(sensor => generateReading(sensor, timestamp, elapsedSeconds));
    const summary = readings.reduce((acc, reading) => {
      acc.total++;
      acc[reading.severity.toLowerCase()] = (acc[reading.severity.toLowerCase()] || 0) + 1;
      if (reading.quality_flag !== "GOOD") acc.quality_issues++;
      return acc;
    }, { total: 0, normal: 0, medium: 0, high: 0, critical: 0, quality_issues: 0 });

    return {
      type: "live_batch",
      timestamp,
      scenario: activeScenario,
      source: "ARIA_DIGITAL_TWIN",
      readings,
      summary,
    };
  }

  return { nextBatch, setScenario, getScenario: () => activeScenario };
}

module.exports = { createDigitalTwin, loadSensorMetadata };
