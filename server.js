// server.js
import express from "express";
import pkg from "pg";
import dotenv from "dotenv";

dotenv.config();

const app = express();
const { Pool } = pkg;

// ---- env ---------------------------------------------------------
const PORT = process.env.PORT || 10000;
const DATABASE_URL = process.env.DATABASE_URL;
const IMPORT_TOKEN = process.env.IMPORT_TOKEN || "";
if (!DATABASE_URL) {
  console.error("Missing DATABASE_URL env var");
  process.exit(1);
}

// ---- db pool -----------------------------------------------------
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ---- boot-time migration (idempotent) ---------------------------
async function initDB() {
  const client = await pool.connect();
  try {
    // Create table if missing (primary key on company_slug+internal_job_id)
    await client.query(`
      CREATE TABLE IF NOT EXISTS jobs (
        company_slug       TEXT NOT NULL,
        internal_job_id    TEXT NOT NULL,
        title              TEXT,
        url                TEXT,
        payload            JSONB,
        created_at         TIMESTAMPTZ DEFAULT now(),
        updated_at         TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (company_slug, internal_job_id)
      );
    `);

    // If the table already existed with a different shape, add what we need.
    await client.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS title TEXT;`);
    await client.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS url TEXT;`);
    await client.query(`ALTER TABLE jobs ADD COLUMN IF NOT EXISTS payload JSONB;`);
    await client.query(
      `ALTER TABLE jobs ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now();`
    );
    await client.query(
      `ALTER TABLE jobs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now();`
    );
    await client.query(
      `CREATE INDEX IF NOT EXISTS idx_jobs_updated_at ON jobs(updated_at);`
    );
  } finally {
    client.release();
  }
}

// ---- middleware --------------------------------------------------
// tiny root
app.get("/", (_req, res) => {
  res.type("text/plain").send("ok");
});

// healthz for Render
app.get("/healthz", (_req, res) => {
  res.json({ ok: true, env: process.env.NODE_ENV || "development" });
});

// DB health
app.get("/api/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, db: "ok", ts: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// Count jobs
app.get("/api/jobs/count", async (_req, res) => {
  try {
    const r = await pool.query("SELECT COUNT(*)::int AS c FROM jobs");
    res.json({ ok: true, count: r.rows[0].c });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err) });
  }
});

// simple import-token guard
function checkToken(req, res, next) {
  const t = req.header("x-import-token") || "";
  if (!IMPORT_TOKEN) return res.status(500).json({ ok: false, error: "IMPORT_TOKEN not set" });
  if (t !== IMPORT_TOKEN) return res.status(401).json({ ok: false, error: "bad token" });
  next();
}

// ---- NDJSON import ----------------------------------------------
// usage:
// curl -sS -X POST "https://<your-service>.onrender.com/api/jobs/ndjson?batch=2000" \
//   -H "x-import-token: <YOUR TOKEN>" \
//   -H "content-type: application/x-ndjson" \
//   --data-binary @your_file.ndjson
//
// Each line must be valid JSON with at least:
// { "company_slug": "...", "internal_job_id": "...", "title": "...", "url": "..." }
// All other fields will be stored in payload (jsonb).

import { createInterface } from "node:readline";
import { pipeline } from "node:stream/promises";

app.post("/api/jobs/ndjson", checkToken, async (req, res) => {
  const batchSize = Math.max(1, Math.min(5000, Number(req.query.batch) || 1000));

  let read = 0;
  let inserted = 0;
  const errors = [];

  // helper: insert a batch with per-row protection
  async function insertBatch(recs) {
    if (recs.length === 0) return;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      for (const r of recs) {
        // required
        const company_slug = (r.company_slug || "").trim();
        const internal_job_id = (r.internal_job_id || "").trim();
        if (!company_slug || !internal_job_id) {
          errors.push({ row: r, error: "missing company_slug/internal_job_id" });
          continue;
        }

        // optional
        const title = r.title ?? null;
        const url = r.url ?? null;
        const updated_at = r.updated_at ? new Date(r.updated_at) : null;

        // keep full record as payload
        const payload = r === null ? null : JSON.stringify(r);

        try {
          await client.query(
            `
            INSERT INTO jobs (company_slug, internal_job_id, title, url, updated_at, payload)
            VALUES ($1,$2,$3,$4,COALESCE($5, now()), $6::jsonb)
            ON CONFLICT (company_slug, internal_job_id)
            DO UPDATE
              SET title = EXCLUDED.title,
                  url   = EXCLUDED.url,
                  updated_at = COALESCE(EXCLUDED.updated_at, jobs.updated_at),
                  payload = COALESCE(EXCLUDED.payload, jobs.payload);
          `,
            [company_slug, internal_job_id, title, url, updated_at, payload]
          );
          inserted++;
        } catch (err) {
          // Critical: reset transaction, keep going (prevents 25P03 fatal)
          try {
            await client.query("ROLLBACK");
            await client.query("BEGIN");
          } catch (_) {}
          errors.push({ row: { company_slug, internal_job_id }, error: String(err?.message || err) });
        }
      }

      await client.query("COMMIT");
    } catch (err) {
      try { await client.query("ROLLBACK"); } catch {}
      errors.push({ batch: "fatal", error: String(err?.message || err) });
    } finally {
      client.release();
    }
  }

  // stream NDJSON
  const rl = createInterface({ input: req, crlfDelay: Infinity });
  let buffer = [];

  rl.on("line", (line) => {
    if (!line || !line.trim()) return;
    read++;
    try {
      const obj = JSON.parse(line);
      buffer.push(obj);
      if (buffer.length >= batchSize) {
        rl.pause();
        insertBatch(buffer.splice(0, buffer.length))
          .then(() => rl.resume())
          .catch((e) => {
            errors.push({ batch: "fatal", error: String(e) });
            rl.resume();
          });
      }
    } catch (e) {
      errors.push({ row: read, error: `invalid JSON: ${String(e.message || e)}` });
    }
  });

  rl.on("close", async () => {
    if (buffer.length) {
      await insertBatch(buffer.splice(0, buffer.length));
    }
    res.json({ ok: true, read, inserted, errorsCount: errors.length, errors: errors.slice(0, 50) });
  });

  // Ensure backpressure is respected
  await pipeline(req, async function* (source) {
    for await (const chunk of source) yield chunk;
  });
});

// ---- hardening ---------------------------------------------------
process.on("unhandledRejection", (e) => console.error("unhandledRejection", e));
process.on("uncaughtException", (e) => console.error("uncaughtException", e));

// ---- start -------------------------------------------------------
initDB()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Server listening on :${PORT}`);
    });
  })
  .catch((e) => {
    console.error("DB init failed:", e);
    process.exit(1);
  });
