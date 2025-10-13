// --- at top of server.js (if not already there)
import express from "express";
import dotenv from "dotenv";
import fetch from "node-fetch";
import { parse } from "csv-parse/sync";
import pg from "pg";
dotenv.config();

const app = express();
app.use(express.json());

const { Pool } = pg;
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }, // required on Render/Neon
});

// --- health check (GET)
/**
 * GET /api/health
 * Confirms the server is up and DB is reachable.
 */
app.get("/api/health", async (req, res) => {
  try {
    const { rows } = await pool.query("select now() as ts");
    res.json({ ok: true, ts: rows[0].ts });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// --- importer (POST)
/**
 * POST /api/import?url=<csvUrl>
 * Header: x-import-token: <your token>
 * Minimal stub that fetches a CSV and returns how many rows were seen.
 * (You can plug in your real INSERT logic later.)
 */
app.post("/api/import", async (req, res) => {
  const token = req.header("x-import-token");
  if (!process.env.IMPORT_TOKEN || token !== process.env.IMPORT_TOKEN) {
    return res.status(401).json({ ok: false, error: "invalid token" });
  }

  const csvUrl = req.query.url;
  if (!csvUrl) return res.status(400).json({ ok: false, error: "missing url" });

  try {
    const r = await fetch(csvUrl);
    if (!r.ok) throw new Error(`fetch failed: ${r.status} ${r.statusText}`);
    const text = await r.text();

    const records = parse(text, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    });

    // TODO: insert records into your table with pool.query(...)
    // For now, just report a count:
    res.json({ ok: true, received: records.length });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// --- listen (keep your existing PORT logic)
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Jobs API running on :${PORT}`));
