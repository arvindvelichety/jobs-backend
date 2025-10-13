// server.js — safe, incremental boot

import express from "express";
import dotenv from "dotenv";
dotenv.config();

import fetch from "node-fetch";
import { parse } from "csv-parse/sync";

// Postgres (works with "type": "module")
import pkg from "pg";
const { Pool } = pkg;

const app = express();
app.use(express.json());

// --- tiny request logger (helps in Render logs)
app.use((req, res, next) => {
  const t0 = Date.now();
  res.on("finish", () => {
    const ms = Date.now() - t0;
    console.log(`${req.method} ${req.originalUrl} -> ${res.statusCode} ${ms}ms`);
  });
  next();
});

// --- DB: make it optional so the app still boots
let pool = null;
if (process.env.DATABASE_URL) {
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }, // Neon/Render friendly
  });
} else {
  console.warn("DATABASE_URL not set — /api/health will skip DB check.");
}

// --- Global crash guards (so the process doesn’t die silently)
process.on("unhandledRejection", (e) => {
  console.error("UNHANDLED REJECTION:", e);
});
process.on("uncaughtException", (e) => {
  console.error("UNCAUGHT EXCEPTION:", e);
});

// ------- ROUTES -------

// Quick liveness probe (no DB)
app.get("/healthz", (_req, res) => {
  res.json({ ok: true, env: process.env.NODE_ENV || "production" });
});

// DB-aware health
app.get("/api/health", async (_req, res) => {
  try {
    if (!pool) return res.json({ ok: true, db: "skipped", ts: null });
    const { rows } = await pool.query("select now() as ts");
    res.json({ ok: true, db: "ok", ts: rows[0].ts });
  } catch (err) {
    console.error("health error:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// CSV importer (parse-only for now; safe)
app.post("/api/import", async (req, res) => {
  try {
    // simple token check
    const token = req.header("x-import-token");
    if (!process.env.IMPORT_TOKEN || token !== process.env.IMPORT_TOKEN) {
      return res.status(401).json({ ok: false, error: "invalid token" });
    }

    const csvUrl = req.query.url;
    if (!csvUrl) {
      return res.status(400).json({ ok: false, error: "missing url query param" });
    }

    // Fetch CSV
    const r = await fetch(csvUrl, { redirect: "follow" });
    if (!r.ok) {
      throw new Error(`fetch failed: ${r.status} ${r.statusText}`);
    }
    const text = await r.text();

    // Parse CSV
    const records = parse(text, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });

    // Optional preview & limit for safety
    const limit = Math.max(0, Math.min(Number(req.query.limit ?? 0), records.length));
    const previewCount = limit || Math.min(records.length, 3);
    const preview = records.slice(0, previewCount);

    // NOTE: We’re NOT inserting yet (to avoid schema mismatches causing 5xx).
    // Once we confirm this returns ok, we’ll enable inserts.

    return res.json({
      ok: true,
      url: csvUrl,
      parsed: records.length,
      previewCount,
      preview, // first few rows so you can confirm the field names
      insert: "skipped (safe mode)",
    });
  } catch (err) {
    console.error("import error:", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// --- Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Jobs API listening on ${PORT}`);
});

// --- Graceful shutdown (Render sends SIGTERM on redeploys)
process.on("SIGTERM", async () => {
  console.log("SIGTERM received, closing server...");
  try {
    if (pool) await pool.end();
  } finally {
    process.exit(0);
  }
});
