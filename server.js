// server.js
import express from 'express';
import dotenv from 'dotenv';
import { Pool } from 'pg';

// ---------- env & config ----------
dotenv.config();
const PORT = process.env.PORT || 10000;

// Render | Neon PG
// Expected envs: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (or DATABASE_URL)
// Also: IMPORT_TOKEN for authenticated imports
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || undefined,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : undefined,
});

// Basic app (no global body parser for NDJSON route)
const app = express();

// Only enable JSON parser for normal JSON routes (not NDJSON)
app.use(express.json({ limit: '10mb' }));

// ---------- helpers ----------
function ok(res, data = {}) {
  return res.json({ ok: true, ...data });
}
function bad(res, msg, extra = {}) {
  return res.status(400).json({ ok: false, error: msg, ...extra });
}
function authOr401(req, res) {
  const importToken = req.get('x-import-token');
  if (!process.env.IMPORT_TOKEN || importToken !== process.env.IMPORT_TOKEN) {
    res.status(401).json({ ok: false, error: 'unauthorized' });
    return false;
  }
  return true;
}

// Small PG ping
async function pingDb() {
  const { rows } = await pool.query('SELECT NOW() AS ts');
  return rows[0].ts;
}

// Upsert one job (minimal required fields + payload passthrough)
async function upsertJob(client, row) {
  // required keys
  const company_slug = (row.company_slug ?? '').toString().trim();
  const internal_job_id = (row.internal_job_id ?? '').toString().trim();
  if (!company_slug || !internal_job_id) {
    throw new Error('missing company_slug or internal_job_id');
  }

  // a few commonly-used top-level columns (optional)
  const title = row.title ?? null;
  const url = row.url ?? null;

  // updated_at: allow ISO string or null; normalize to timestamp or null
  // Postgres will cast text to timestamp when possible.
  const updated_at = row.updated_at ?? null;

  // Keep **all** original fields in payload for flexibility/search later
  const payload = row;

  // NOTE: We do NOT reference columns that might not exist in your table.
  // If your schema has additional columns (e.g., updated_row_at), use triggers
  // or add them explicitly.
  await client.query(
    `
    INSERT INTO jobs (company_slug, internal_job_id, title, url, updated_at, payload)
    VALUES ($1, $2, $3, $4, $5, $6::jsonb)
    ON CONFLICT (company_slug, internal_job_id)
    DO UPDATE SET
      title      = EXCLUDED.title,
      url        = EXCLUDED.url,
      updated_at = EXCLUDED.updated_at,
      payload    = EXCLUDED.payload
    `,
    [company_slug, internal_job_id, title, url, updated_at, JSON.stringify(payload)]
  );
}

// ---------- routes ----------

// Health
app.get('/api/health', async (_req, res) => {
  try {
    const ts = await pingDb();
    ok(res, { db: 'ok', ts });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Count via view (preferred) or fallback to table if view is absent
app.get('/api/jobs/count', async (_req, res) => {
  try {
    let q = 'SELECT COUNT(*)::int AS count FROM jobs_view';
    try {
      const { rows } = await pool.query(q);
      ok(res, { count: rows[0].count });
    } catch {
      // fallback if the view doesn't exist
      const { rows } = await pool.query('SELECT COUNT(*)::int AS count FROM jobs');
      ok(res, { count: rows[0].count });
    }
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// List jobs (reads from jobs_view for nicer shape)
app.get('/api/jobs', async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(1000, parseInt(req.query.limit, 10) || 20));
    const offset = Math.max(0, parseInt(req.query.offset, 10) || 0);

    let q = `
      SELECT *
      FROM jobs_view
      ORDER BY created_at DESC
      LIMIT $1 OFFSET $2
    `;
    try {
      const { rows } = await pool.query(q, [limit, offset]);
      ok(res, { limit, offset, rows: rows.length, jobs: rows });
    } catch {
      // fallback: show raw jobs if view not present
      const { rows } = await pool.query(
        `
        SELECT *
        FROM jobs
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
        `,
        [limit, offset]
      );
      ok(res, { limit, offset, rows: rows.length, jobs: rows });
    }
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Secure NDJSON bulk upsert
app.post('/api/jobs/ndjson', async (req, res) => {
  try {
    if (!authOr401(req, res)) return;

    // Read raw body (don’t use express.json here)
    let body = '';
    await new Promise((resolve, reject) => {
      req.setEncoding('utf8');
      req.on('data', (chunk) => (body += chunk));
      req.on('end', resolve);
      req.on('error', reject);
    });

    const lines = body
      .split(/\r?\n/)
      .map((s) => s.trim())
      .filter(Boolean);

    if (lines.length === 0) {
      return bad(res, 'no NDJSON lines found');
    }

    const batch = Math.max(1, Math.min(5000, parseInt(req.query.batch, 10) || 1000));

    let read = 0;
    let inserted = 0;
    let errors = [];

    // Process in a single transaction for atomicity
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      for (let i = 0; i < lines.length; i += batch) {
        const chunk = lines.slice(i, i + batch);
        for (let j = 0; j < chunk.length; j++) {
          const n = i + j + 1;
          read++;
          let obj;
          try {
            obj = JSON.parse(chunk[j]);
          } catch (e) {
            errors.push({ row: n, error: 'invalid JSON', detail: e.message });
            continue;
          }
          try {
            await upsertJob(client, obj);
            inserted++;
          } catch (e) {
            // extract safe summary of the offending row
            const rowSummary = {
              company_slug: obj?.company_slug ?? null,
              internal_job_id: obj?.internal_job_id ?? null,
            };
            errors.push({ row: n, rowSummary, error: e.message });
          }
        }
      }

      if (errors.length > 0) {
        // If you prefer partial success, you could COMMIT and return with errors.
        // For now, commit regardless to keep successfully parsed rows.
        await client.query('COMMIT');
      } else {
        await client.query('COMMIT');
      }
    } catch (txErr) {
      await client.query('ROLLBACK');
      throw txErr;
    } finally {
      client.release();
    }

    res.json({
      ok: true,
      read,
      inserted,
      errorsCount: errors.length,
      ...(errors.length ? { errors } : {}),
    });
  } catch (err) {
    console.error('POST /api/jobs/ndjson failed:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Simple root page (helps Render health)
app.get('/', (_req, res) => {
  res.type('text/plain').send('ok');
});

// ---------- start ----------
app.listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  try {
    await pool.end();
  } finally {
    process.exit(0);
  }
});
