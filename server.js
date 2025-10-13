// server.js — diagnostic-safe version

import express from "express";
import dotenv from "dotenv";
dotenv.config();

import fetch from "node-fetch";
import { parse } from "csv-parse/sync";
import pkg from "pg";
const { Pool } = pkg;

const app = express();
app.use(express.json());

// log every request
app.use((req, res, next) => {
  const t0 = Date.now();
  res.on("finish", () => {
    console.log(`${req.method} ${req.originalUrl} -> ${res.statusCode} ${Date.now()-t0}ms`);
  });
  next();
});

// DB optional
let pool = null;
if (process.env.DATABASE_URL) {
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });
} else {
  console.warn("DATABASE_URL not set — DB checks will be skipped.");
}

// crash guards
process.on("unhandledRejection", (e) => console.error("UNHANDLED REJECTION:", e));
process.on("uncaughtException", (e) => console.error("UNCAUGHT EXCEPTION:", e));

// --- DIAGNOSTIC: list routes so we know what’s live
const routes = [];
const add = (method, path, handler) => {
  routes.push({ method, path });
  app[method](path, handler);
};

// root
add("get", "/", (_req, res) => {
  res.json({
    ok: true,
    message: "Jobs API is running",
    routes,
    node: process.version,
    port: process.env.PORT || 3000,
  });
});

// liveness (no DB)
add("get", "/healthz", (_req, res) => {
  res.json({ ok: true, env: process.env.NODE_ENV || "production" });
});

// DB health
add("get", "/api/health", async (_req, res) => {
  try {
    if (!pool) return res.json({ ok: true, db: "skipped", ts: null });
    const { rows } = await pool.query("select now() as ts");
    res.json({ ok: true, db: "ok", ts: rows[0].ts });
  } catch (err) {
    console.error("health error:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// importer (parse-only for now)
add("post", "/api/import", async (req, res) => {
  try {
    const token = req.header("x-import-token");
    if (!process.env.IMPORT_TOKEN || token !== process.env.IMPORT_TOKEN) {
      return res.status(401).json({ ok: false, error: "invalid token" });
    }
    const csvUrl = req.query.url;
    if (!csvUrl) return res.status(400).json({ ok: false, error: "missing url" });

    const r = await fetch(csvUrl, { redirect: "follow" });
    if (!r.ok) throw new Error(`fetch failed: ${r.status} ${r.statusText}`);
    const text = await r.text();

    const records = parse(text, { columns: true, skip_empty_lines: true, trim: true });
    const preview = records.slice(0, Math.min(3, records.length));

    res.json({ ok: true, parsed: records.length, previewCount: preview.length, preview, insert: "skipped" });
  } catch (err) {
    console.error("import error:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// helpful 404 that shows available routes
app.use((req, res) => {
  res.status(404).json({ ok: false, error: "not found", path: req.originalUrl, routes });
});

// start
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Jobs API listening on ${PORT}`);
});

// graceful shutdown
process.on("SIGTERM", async () => {
  console.log("SIGTERM received");
  try { if (pool) await pool.end(); } finally { process.exit(0); }
});
