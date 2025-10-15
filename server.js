// server.js
import express from 'express';
import dotenv from 'dotenv';
import pkg from 'pg';

dotenv.config();
const { Pool } = pkg;

const app = express();

// We’ll use body parsers for JSON routes, but the NDJSON route reads raw.
app.use(express.json({ limit: '50mb' }));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Render/Neon friendly defaults:
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

// simple health
app.get(['/healthz', '/api/health'], async (req, res) => {
  try {
    await pool.query('select 1');
    res.json({ ok: true, db: 'ok', ts: new Date().toISOString() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// count
app.get('/api/jobs/count', async (_req, res) => {
  try {
    const r = await pool.query('select count(*)::int as c from jobs');
    res.json({ ok: true, count: r.rows[0].c });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// list (very simple)
app.get('/api/jobs', async (req, res) => {
  const limit = Math.min(parseInt(req.query.limit ?? '20', 10), 100);
  const offset = parseInt(req.query.offset ?? '0', 10);

  try {
    const r = await pool.query(
      `select
         company_slug, internal_job_id, company_name, title, url,
         location_text, employment_type, workplace_type, updated_at,
         salary_currency, salary_min, salary_max,
         latitude, longitude, content_text
       from jobs
       order by updated_at desc nulls last
       limit $1 offset $2`,
      [limit, offset]
    );

    res.json({ ok: true, limit, offset, rows: r.rowCount, jobs: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// NDJSON bulk insert (POST /api/jobs/ndjson)
// content-type: application/x-ndjson
app.post('/api/jobs/ndjson', async (req, res) => {
  try {
    const importToken = req.get('x-import-token');
    if (!process.env.IMPORT_TOKEN || importToken !== process.env.IMPORT_TOKEN) {
      return res.status(401).json({ ok: false, error: 'unauthorized' });
    }

    // Read raw NDJSON body
    let body = '';
    await new Promise((resolve, reject) => {
      req.setEncoding('utf8');
      req.on('data', chunk => (body += chunk));
      req.on('end', resolve);
      req.on('error', reject);
    });

    const lines = body.split(/\r?\n/).filter(Boolean);
    const jobs = [];
    const parseErrors = [];

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      try {
        const obj = JSON.parse(line);

        // Map only the fields we keep in the core table
        jobs.push({
          company_slug: obj.company_slug ?? null,
          internal_job_id: obj.internal_job_id ?? null,
          company_name: obj.company_name ?? null,
          title: obj.title ?? null,
          url: obj.url ?? null,
          location_text: obj.location_text ?? null,
          employment_type: obj.employment_type ?? null,  // FULL_TIME / PART_TIME ...
          workplace_type: obj.workplace_type ?? null,    // on_site / hybrid / remote
          updated_at: obj.updated_at ?? null,
          salary_currency: obj.salary_currency ?? null,
          salary_min: obj.salary_min ?? null,
          salary_max: obj.salary_max ?? null,
          latitude: obj.latitude ?? null,
          longitude: obj.longitude ?? null,
          content_text: obj.content_text ?? null,
          extras: buildExtras(obj), // stash anything else so it isn’t lost
        });
      } catch (e) {
        parseErrors.push({ line: i + 1, error: 'invalid json' });
      }
    }

    if (jobs.length === 0) {
      return res.json({ ok: true, read: lines.length, inserted: 0, errorsCount: parseErrors.length, errors: parseErrors });
    }

    // insert in a single transaction
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // ensure table exists (safe to run each import)
      await client.query(`
        CREATE TABLE IF NOT EXISTS jobs (
          company_slug    text NOT NULL,
          internal_job_id text NOT NULL,
          company_name    text,
          title           text,
          url             text,
          location_text   text,
          employment_type text,
          workplace_type  text,
          updated_at      timestamptz,
          salary_currency text,
          salary_min      numeric,
          salary_max      numeric,
          latitude        numeric,
          longitude       numeric,
          content_text    text,
          extras          jsonb,
          created_at      timestamptz DEFAULT now(),
          PRIMARY KEY (company_slug, internal_job_id)
        );
      `);

      // upsert rows
      const sql = `
        INSERT INTO jobs (
          company_slug, internal_job_id, company_name, title, url,
          location_text, employment_type, workplace_type, updated_at,
          salary_currency, salary_min, salary_max,
          latitude, longitude, content_text, extras
        )
        VALUES (
          $1,$2,$3,$4,$5,
          $6,$7,$8,$9,
          $10,$11,$12,
          $13,$14,$15,$16
        )
        ON CONFLICT (company_slug, internal_job_id) DO UPDATE SET
          company_name    = EXCLUDED.company_name,
          title           = EXCLUDED.title,
          url             = EXCLUDED.url,
          location_text   = EXCLUDED.location_text,
          employment_type = EXCLUDED.employment_type,
          workplace_type  = EXCLUDED.workplace_type,
          updated_at      = EXCLUDED.updated_at,
          salary_currency = EXCLUDED.salary_currency,
          salary_min      = EXCLUDED.salary_min,
          salary_max      = EXCLUDED.salary_max,
          latitude        = EXCLUDED.latitude,
          longitude       = EXCLUDED.longitude,
          content_text    = EXCLUDED.content_text,
          extras          = COALESCE(jobs.extras, '{}'::jsonb) || EXCLUDED.extras
      `;

      let inserted = 0;
      for (const j of jobs) {
        // require identity
        if (!j.company_slug || !j.internal_job_id) { continue; }

        await client.query(sql, [
          j.company_slug, j.internal_job_id, j.company_name, j.title, j.url,
          j.location_text, j.employment_type, j.workplace_type, j.updated_at,
          j.salary_currency, j.salary_min, j.salary_max,
          j.latitude, j.longitude, j.content_text, j.extras
        ]);
        inserted++;
      }

      await client.query('COMMIT');
      client.release();

      return res.json({ ok: true, read: lines.length, inserted, errorsCount: parseErrors.length, errors: parseErrors });
    } catch (e) {
      try { await client.query('ROLLBACK'); } catch {}
      client.release();
      return res.status(500).json({ ok: false, error: e.message });
    }
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// tiny helpers
function buildExtras(obj) {
  const keep = new Set([
    'company_slug','internal_job_id','company_name','title','url',
    'location_text','employment_type','workplace_type','updated_at',
    'salary_currency','salary_min','salary_max','latitude','longitude','content_text'
  ]);
  const extras = {};
  for (const k of Object.keys(obj)) {
    if (!keep.has(k)) extras[k] = obj[k];
  }
  return Object.keys(extras).length ? extras : null;
}

// root
app.get('/', (_req, res) => res.json({ ok: true, service: 'jobs-backend' }));

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`jobs-backend listening on :${port}`);
});
