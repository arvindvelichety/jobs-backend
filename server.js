// server.js  — full, safe version
// Node 18+, "type": "module" in package.json

import express from "express";
import dotenv from "dotenv";
import fetch from "node-fetch";
import { parse as csvParse } from "csv-parse";
import { createInterface } from "readline";
import pg from "pg";

dotenv.config();

const { Pool } = pg;
const PORT = process.env.PORT || 10000;
const DATABASE_URL = process.env.DATABASE_URL;
const IMPORT_TOKEN = process.env.IMPORT_TOKEN || "";
const INSERT_MODE = process.env.INSERT_MODE || "1"; // 1 = upsert, anything else = insert only

// --- DB pool (Render/Neon friendly SSL) ---
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL?.includes("sslmode=require")
    ? true
    : { rejectUnauthorized: false },
});
pool.on("error", (err) => console.error("PG pool error:", err));

// --- ensure table on boot ---
async function ensureTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS jobs (
      id BIGSERIAL PRIMARY KEY,
      company_slug TEXT NOT NULL,
      internal_job_id TEXT NOT NULL,
      title TEXT,
      url TEXT,
      updated_at TIMESTAMPTZ DEFAULT now(),
      payload JSONB,
      created_at TIMESTAMPTZ DEFAULT now(),
      updated_row_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE (company_slug, internal_job_id)
    );
  `);
}
ensureTables().catch((e) => {
  console.error("ensureTables failed:", e);
  process.exit(1);
});

const app = express();

// health + root
app.get("/", (_req, res) => {
  res.type("text/plain").send("ok\n");
});
app.get("/healthz", (_req, res) => {
  res.json({ ok: true, env: process.env.NODE_ENV || "development" });
});
app.get("/api/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, db: "ok", ts: new Date().toISOString() });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// jobs count
app.get("/api/jobs/count", async (_req, res) => {
  try {
    const r = await pool.query("SELECT COUNT(*)::int AS c FROM jobs");
    res.json({ ok: true, count: r.rows[0]?.c ?? 0 });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// simple token check for write endpoints
function checkToken(req, res, next) {
  const t = req.headers["x-import-token"];
  if (!IMPORT_TOKEN) {
    return res.status(500).json({ ok: false, error: "IMPORT_TOKEN not set" });
  }
  if (!t || t !== IMPORT_TOKEN) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }
  next();
}

// ---- CSV import over URL ----------------------------------------------------
// Usage:
// curl -X POST "https://<service>.onrender.com/api/import?url=ENCODED_URL&dryRun=true" \
//   -H "x-import-token: YOUR_TOKEN"
app.post("/api/import", checkToken, async (req, res) => {
  const url = req.query.url;
  const dryRun = String(req.query.dryRun ?? "true").toLowerCase() !== "false";
  if (!url) return res.status(400).json({ ok: false, error: "missing ?url" });

  let resp;
  try {
    resp = await fetch(url.toString(), { redirect: "follow" });
  } catch (e) {
    return res.status(400).json({ ok: false, error: `fetch failed: ${String(e)}` });
  }
  if (!resp.ok) {
    return res.status(400).json({ ok: false, error: `source returned ${resp.status}` });
  }

  const previews = [];
  let parsed = 0;
  const rows = [];

  const parser = csvParse({
    columns: true,          // map to object by header
    bom: true,
    trim: true,
    skip_empty_lines: true,
    relax_column_count: true, // be tolerant of extra commas
  });

  // helper to normalize fields from CSV
  const get = (row, key) => {
    const v = (row[key] ?? "").toString().trim();
    return v === "" ? null : v;
  };

  async function insertBatch(batch) {
    if (!batch.length || dryRun) return;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (const r of batch) {
        const company_slug = (r.company_slug || "").trim();
        const internal_job_id = (r.internal_job_id || "").trim();
        if (!company_slug || !internal_job_id) {
          throw new Error("required company_slug/internal_job_id missing");
        }
        const title = r.title ?? null;
        const url = r.url ?? null;
        const updated_at = r.updated_at ? new Date(r.updated_at) : null;
        const payload = JSON.stringify(r);

        const upsert = `
          INSERT INTO jobs (company_slug, internal_job_id, title, url, updated_at, payload)
          VALUES ($1,$2,$3,$4,COALESCE($5, now()), $6::jsonb)
          ON CONFLICT (company_slug, internal_job_id)
          ${INSERT_MODE === "1"
            ? `DO UPDATE SET title=EXCLUDED.title, url=EXCLUDED.url,
                             updated_at=COALESCE(EXCLUDED.updated_at, jobs.updated_at),
                             payload=COALESCE(EXCLUDED.payload, jobs.payload),
                             updated_row_at=now()`
            : "DO NOTHING"};
        `;
        await client.query(upsert, [company_slug, internal_job_id, title, url, updated_at, payload]);
      }
      await client.query("COMMIT");
    } catch (e) {
      try { await client.query("ROLLBACK"); } catch {}
      throw e;
    } finally {
      client.release();
    }
  }

  try {
    let batch = [];
    const BATCH = 1000;

    await new Promise((resolve, reject) => {
      resp.body.pipe(parser)
        .on("readable", () => {
          let row;
          while ((row = parser.read()) !== null) {
            parsed++;

            // map CSV into normalized record, honoring header names
            const rec = {
              company_slug: get(row, "company_slug"),
              company_name: get(row, "company_name"),
              job_id: get(row, "job_id"),
              internal_job_id: get(row, "internal_job_id"),
              title: get(row, "title"),
              url: get(row, "url"),
              location_text: get(row, "location_text"),
              departments: get(row, "departments"),
              offices: get(row, "offices"),
              employment_type: get(row, "employment_type"),
              workplace_type: get(row, "workplace_type"),
              remote_hint: get(row, "remote_hint"),
              shift: get(row, "shift"),
              updated_at: get(row, "updated_at"),
              requisition_open_date: get(row, "requisition_open_date"),
              job_category: get(row, "job_category"),
              benefits: get(row, "benefits"),
              travel_requirement: get(row, "travel_requirement"),
              years_experience: get(row, "years_experience"),
              experience_range: get(row, "experience_range"),
              contractor_type: get(row, "contractor_type"),
              address: get(row, "address"),
              city: get(row, "city"),
              state: get(row, "state"),
              country: get(row, "country"),
              latitude: get(row, "latitude"),
              longitude: get(row, "longitude"),
              salary_currency: get(row, "salary_currency"),
              salary_min: get(row, "salary_min"),
              salary_max: get(row, "salary_max"),
              content_text: get(row, "content_text"),
              content_html: get(row, "content_html"),
            };

            if (previews.length < 2) previews.push(rec);

            // validate required keys (match our DB unique key)
            if (!rec.company_slug || !rec.internal_job_id) {
              // skip invalid rows but keep a preview
            } else {
              batch.push(rec);
              if (batch.length >= BATCH) {
                parser.pause();
                insertBatch(batch)
                  .then(() => { batch = []; parser.resume(); })
                  .catch(reject);
              }
            }
          }
        })
        .once("end", async () => {
          try {
            if (batch.length) await insertBatch(batch);
            resolve();
          } catch (e) {
            reject(e);
          }
        })
        .once("error", reject);
    });

    res.json({
      ok: true,
      parsed,
      dryRun,
      previewCount: previews.length,
      preview: previews,
      insert: dryRun ? "skipped" : "done",
    });
  } catch (e) {
    res.status(400).json({ ok: false, error: String(e?.message || e) });
  }
});

// ---- NDJSON streaming (fixed; no double-read) -------------------------------
// Each line must be a valid JSON object with at least company_slug & internal_job_id.
app.post("/api/jobs/ndjson", checkToken, async (req, res) => {
  const batchSize = Math.max(1, Math.min(5000, Number(req.query.batch) || 1000));

  let read = 0;
  let inserted = 0;
  const errors = [];

  async function insertBatch(recs) {
    if (!recs.length) return;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      for (const r of recs) {
        const company_slug = (r.company_slug || "").trim();
        const internal_job_id = (r.internal_job_id || "").trim();
        if (!company_slug || !internal_job_id) {
          errors.push({ row: r, error: "missing company_slug/internal_job_id" });
          continue;
        }
        const title = r.title ?? null;
        const url = r.url ?? null;
        const updated_at = r.updated_at ? new Date(r.updated_at) : null;
        const payload = JSON.stringify(r);

        try {
          await client.query(
            `INSERT INTO jobs (company_slug, internal_job_id, title, url, updated_at, payload)
             VALUES ($1,$2,$3,$4,COALESCE($5, now()), $6::jsonb)
             ON CONFLICT (company_slug, internal_job_id)
             ${INSERT_MODE === "1"
               ? `DO UPDATE SET title=EXCLUDED.title, url=EXCLUDED.url,
                                updated_at=COALESCE(EXCLUDED.updated_at, jobs.updated_at),
                                payload=COALESCE(EXCLUDED.payload, jobs.payload),
                                updated_row_at=now()`
               : "DO NOTHING"};`,
            [company_slug, internal_job_id, title, url, updated_at, payload]
          );
          inserted++;
        } catch (e) {
          try { await client.query("ROLLBACK"); await client.query("BEGIN"); } catch {}
          errors.push({ row: { company_slug, internal_job_id }, error: String(e?.message || e) });
        }
      }
      await client.query("COMMIT");
    } catch (e) {
      try { await client.query("ROLLBACK"); } catch {}
      errors.push({ batch: "fatal", error: String(e?.message || e) });
    } finally {
      client.release();
    }
  }

  const rl = createInterface({ input: req, crlfDelay: Infinity });
  let buffer = [];

  rl.on("line", (line) => {
    if (!line.trim()) return;
    read++;
    try {
      buffer.push(JSON.parse(line));
      if (buffer.length >= batchSize) {
        rl.pause();
        insertBatch(buffer.splice(0, buffer.length))
          .then(() => rl.resume())
          .catch((e) => { errors.push({ batch: "fatal", error: String(e) }); rl.resume(); });
      }
    } catch (e) {
      errors.push({ row: read, error: `invalid JSON: ${String(e.message || e)}` });
    }
  });

  rl.once("close", async () => {
    if (buffer.length) await insertBatch(buffer.splice(0, buffer.length));
    res.json({ ok: true, read, inserted, errorsCount: errors.length, errors: errors.slice(0, 50) });
  });

  req.once("error", (e) => {
    errors.push({ stream: "request", error: String(e) });
  });
});

// --- start server ---
app.listen(PORT, () => {
  console.log(`jobs-backend listening on :${PORT}`);
});
