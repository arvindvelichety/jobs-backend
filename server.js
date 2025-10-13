// --- server.js ---
// Imports
import express from "express";
import pkg from "pg";
import dotenv from "dotenv";
import fetch from "node-fetch";
import { parse } from "csv-parse";

dotenv.config();
const { Pool } = pkg;

// --- Setup express app ---
const app = express();
app.use(express.json());

// --- Connect to Neon (or any Postgres) ---
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

pool
  .connect()
  .then(() => console.log("✅ Connected to database"))
  .catch((err) => console.error("❌ DB connection failed:", err));

// --- Config flags ---
const INSERT_MODE =
  String(process.env.INSERT_MODE || "").toLowerCase() === "1" ||
  String(process.env.INSERT_MODE || "").toLowerCase() === "true";

function parseBool(v, def = false) {
  if (v === undefined || v === null || v === "") return def;
  const s = String(v).trim().toLowerCase();
  return ["1", "true", "yes", "on"].includes(s)
    ? true
    : ["0", "false", "no", "off"].includes(s)
    ? false
    : def;
}

// --- verify token middleware ---
function requireImportToken(req, res, next) {
  const header = req.get("x-import-token") || "";
  const expected = process.env.IMPORT_TOKEN || "";
  if (!expected)
    return res.status(500).json({ ok: false, error: "IMPORT_TOKEN not set" });
  if (header !== expected)
    return res.status(401).json({ ok: false, error: "bad token" });
  next();
}

// --- Health check routes ---
app.get("/", (req, res) => res.send("Backend is running ✅"));
app.get("/healthz", (req, res) => res.json({ ok: true, env: process.env.NODE_ENV || "dev" }));

app.get("/api/health", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() AS now");
    res.json({ ok: true, db: "ok", ts: r.rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --- Import route ---
app.post("/api/import", requireImportToken, async (req, res) => {
  try {
    const url = req.query.url || req.body?.url;
    if (!url) return res.status(400).json({ ok: false, error: "missing url" });

    const dryRunParam = req.query.dryRun ?? req.body?.dryRun;
    const dryRun = parseBool(dryRunParam, !INSERT_MODE);

    const r = await fetch(url, { redirect: "follow" });
    const ctype = r.headers.get("content-type") || "";
    const text = await r.text();

    if (ctype.includes("html") || text.trim().startsWith("<!DOCTYPE")) {
      return res
        .status(415)
        .json({ ok: false, error: "URL returned HTML, not CSV", contentType: ctype });
    }

    // --- Parse CSV ---
    const records = [];
    await new Promise((resolve, reject) => {
      parse(text, { columns: true, skip_empty_lines: true, trim: true })
        .on("readable", function () {
          let record;
          while ((record = this.read())) records.push(record);
        })
        .on("error", reject)
        .on("end", resolve);
    });

    const preview = records.slice(0, 2);
    let inserted = 0;

    if (!dryRun) {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS jobs (
          job_id TEXT PRIMARY KEY,
          company_slug TEXT,
          company_name TEXT,
          internal_job_id TEXT,
          title TEXT,
          url TEXT,
          location_text TEXT,
          departments TEXT,
          offices TEXT,
          employment_type TEXT,
          workplace_type TEXT,
          remote_hint TEXT,
          shift TEXT,
          updated_at TIMESTAMP NULL,
          requisition_open_date TIMESTAMP NULL,
          job_category TEXT,
          benefits TEXT,
          travel_requirement TEXT,
          years_experience INT NULL,
          experience_range TEXT,
          contractor_type TEXT,
          address TEXT,
          city TEXT,
          state TEXT,
          country TEXT,
          latitude DOUBLE PRECISION NULL,
          longitude DOUBLE PRECISION NULL,
          salary_currency TEXT,
          salary_min NUMERIC NULL,
          salary_max NUMERIC NULL,
          content_text TEXT,
          content_html TEXT
        );
      `);

      for (const r of records) {
        await pool.query(
          `
          INSERT INTO jobs (
            job_id, company_slug, company_name, internal_job_id, title, url, location_text,
            departments, offices, employment_type, workplace_type, remote_hint, shift,
            updated_at, requisition_open_date, job_category, benefits, travel_requirement,
            years_experience, experience_range, contractor_type, address, city, state, country,
            latitude, longitude, salary_currency, salary_min, salary_max, content_text, content_html
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
            NULLIF($14,'')::timestamp, NULLIF($15,'')::timestamp, $16, $17, $18,
            NULLIF($19,'')::int, $20, $21, $22, $23, $24, $25,
            NULLIF($26,'')::double precision, NULLIF($27,'')::double precision, $28,
            NULLIF($29,'')::numeric, NULLIF($30,'')::numeric, $31, $32
          )
          ON CONFLICT (job_id) DO UPDATE SET
            title = EXCLUDED.title,
            url = EXCLUDED.url,
            location_text = EXCLUDED.location_text,
            updated_at = EXCLUDED.updated_at,
            salary_min = EXCLUDED.salary_min,
            salary_max = EXCLUDED.salary_max,
            content_text = EXCLUDED.content_text,
            content_html = EXCLUDED.content_html;
          `,
          [
            r.job_id, r.company_slug, r.company_name, r.internal_job_id, r.title, r.url, r.location_text,
            r.departments, r.offices, r.employment_type, r.workplace_type, r.remote_hint, r.shift,
            r.updated_at, r.requisition_open_date, r.job_category, r.benefits, r.travel_requirement,
            r.years_experience, r.experience_range, r.contractor_type, r.address, r.city, r.state, r.country,
            r.latitude, r.longitude, r.salary_currency, r.salary_min, r.salary_max, r.content_text, r.content_html
          ]
        );
        inserted++;
      }
    }

    res.json({
      ok: true,
      parsed: records.length,
      inserted,
      dryRun,
      previewCount: preview.length,
      preview,
    });
  } catch (err) {
    console.error("/api/import error", err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// --- Job count route (quick verify) ---
app.get("/api/jobs/count", async (req, res) => {
  try {
    const r = await pool.query("SELECT COUNT(*)::int AS n FROM jobs");
    res.json({ ok: true, count: r.rows[0].n });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --- Start server ---
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
