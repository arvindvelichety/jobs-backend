// server.js
import express from 'express';
import dotenv from 'dotenv';
import pkg from 'pg';

dotenv.config();

const { Pool } = pkg;

// ---- DB -------------------------------------------------------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // For Neon/Render you usually want SSL
  ssl: process.env.PGSSLMODE === 'disable' ? false : { rejectUnauthorized: false },
});

// quick connectivity check at boot
pool.on('error', (err) => {
  console.error('Unexpected PG error', err);
  process.exit(1);
});

// ---- APP ------------------------------------------------------
const app = express();

// Accept JSON bodies and NDJSON streaming
app.use(express.json({ limit: '10mb' }));

// NOTE: for NDJSON we’ll manually parse lines; see route below.

// ---- HEALTH ---------------------------------------------------
app.get('/', (_req, res) => {
  res.type('text/plain').send('ok');
});

app.get('/healthz', (_req, res) => {
  res.json({ ok: true, env: process.env.NODE_ENV || 'development' });
});

app.get('/api/health', async (_req, res) => {
  try {
    await pool.query('select 1');
    res.json({ ok: true, db: 'ok', ts: new Date().toISOString() });
  } catch (err) {
    console.error('/api/health error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ---- JOBS: COUNT ----------------------------------------------
app.get('/api/jobs/count', async (_req, res) => {
  try {
    const { rows } = await pool.query('select count(*)::int as c from jobs');
    res.json({ ok: true, count: rows[0].c });
  } catch (err) {
    console.error('GET /api/jobs/count error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ---- JOBS: LIST -----------------------------------------------
// GET /api/jobs?limit=20&offset=0
app.get('/api/jobs', async (req, res) => {
  try {
    const limit = Math.min(Math.max(parseInt(req.query.limit || '20', 10), 1), 100);
    const offset = Math.max(parseInt(req.query.offset || '0', 10), 0);

    const { rows } = await pool.query(
      `
      SELECT
        company_slug, company_name, job_id, internal_job_id, title, url,
        location_text, departments, offices, employment_type, workplace_type,
        remote_hint, shift, updated_at, requisition_open_date, job_category,
        benefits, travel_requirement, years_experience, experience_range,
        contractor_type, address, city, state, country, latitude, longitude,
        salary_currency, salary_min, salary_max, content_text, content_html
      FROM jobs
      ORDER BY COALESCE(updated_row_at, updated_at, created_at, NOW()) DESC
      LIMIT $1 OFFSET $2
      `,
      [limit, offset]
    );

    res.json({ ok: true, limit, offset, rows: rows.length, jobs: rows });
  } catch (err) {
    console.error('GET /api/jobs error:', err);
    res.status(500).json({ ok: false, error: err.message });
  }
});

// ---- JOBS: NDJSON BULK IMPORT --------------------------------
// POST /api/jobs/ndjson?batch=1000
// Header: x-import-token: <your token>
// Body: application/x-ndjson (one JSON object per line)
app.post('/api/jobs/ndjson', async (req, res) => {
  // 1) Guard by token
  const token = req.header('x-import-token') || '';
  if (!process.env.IMPORT_TOKEN || token !== process.env.IMPORT_TOKEN) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }

  // 2) Ensure correct content type
  const ct = (req.headers['content-type'] || '').toLowerCase();
  if (!ct.includes('application/x-ndjson')) {
    return res.status(415).json({ ok: false, error: 'content-type must be application/x-ndjson' });
  }

  // 3) Stream in the NDJSON and split to lines robustly
  const chunks = [];
  req.on('data', (c) => chunks.push(c));
  req.on('end', async () => {
    const text = Buffer.concat(chunks).toString('utf8');
    // Split on CRLF/CR/LF safely
    const lines = text.split(/\r\n|\n|\r/).filter(Boolean);

    // 4) Prepare batch insert
    const batchSize = Math.max(1, Math.min(parseInt(req.query.batch || '1000', 10), 5000));

    // Mapping helper: ensure types
    const toNullIfEmpty = (v) => (v === undefined || v === null ? null : v);
    const toNumberOrNull = (v) => (v === undefined || v === null || v === '' ? null : Number(v));

    // Columns we expect (must match your DB schema)
    const COLS = [
      'company_slug', 'company_name', 'job_id', 'internal_job_id', 'title', 'url',
      'location_text', 'departments', 'offices', 'employment_type', 'workplace_type',
      'remote_hint', 'shift', 'updated_at', 'requisition_open_date', 'job_category',
      'benefits', 'travel_requirement', 'years_experience', 'experience_range',
      'contractor_type', 'address', 'city', 'state', 'country', 'latitude', 'longitude',
      'salary_currency', 'salary_min', 'salary_max', 'content_text', 'content_html'
    ];

    let read = 0;
    let inserted = 0;
    const errors = [];

    // build parameterized INSERT once per batch
    const insertBatch = async (batch) => {
      if (batch.length === 0) return;

      // Build values and placeholders
      const values = [];
      const placeholders = [];

      batch.forEach((row, i) => {
        // sanitize/coerce
        const obj = {
          company_slug: row.company_slug,
          company_name: toNullIfEmpty(row.company_name),
          job_id: toNullIfEmpty(row.job_id),
          internal_job_id: row.internal_job_id,
          title: toNullIfEmpty(row.title),
          url: toNullIfEmpty(row.url),
          location_text: toNullIfEmpty(row.location_text),
          departments: row.departments ?? null, // expect JSON/array in NDJSON
          offices: row.offices ?? null,         // expect JSON/array in NDJSON
          employment_type: toNullIfEmpty(row.employment_type),
          workplace_type: toNullIfEmpty(row.workplace_type),
          remote_hint: toNullIfEmpty(row.remote_hint),
          shift: toNullIfEmpty(row.shift),
          updated_at: toNullIfEmpty(row.updated_at),
          requisition_open_date: toNullIfEmpty(row.requisition_open_date),
          job_category: toNullIfEmpty(row.job_category),
          benefits: row.benefits ?? null,       // expect JSON/array in NDJSON
          travel_requirement: toNullIfEmpty(row.travel_requirement),
          years_experience: toNumberOrNull(row.years_experience),
          experience_range: toNullIfEmpty(row.experience_range),
          contractor_type: toNullIfEmpty(row.contractor_type),
          address: toNullIfEmpty(row.address),
          city: toNullIfEmpty(row.city),
          state: toNullIfEmpty(row.state),
          country: toNullIfEmpty(row.country),
          latitude: toNumberOrNull(row.latitude),
          longitude: toNumberOrNull(row.longitude),
          salary_currency: toNullIfEmpty(row.salary_currency),
          salary_min: toNumberOrNull(row.salary_min),
          salary_max: toNumberOrNull(row.salary_max),
          content_text: toNullIfEmpty(row.content_text),
          content_html: toNullIfEmpty(row.content_html),
        };

        // basic required fields
        if (!obj.company_slug || !obj.internal_job_id) {
          errors.push({ row: { company_slug: obj.company_slug, internal_job_id: obj.internal_job_id }, error: 'company_slug and internal_job_id are required' });
          return;
        }

        // push values in COLS order
        COLS.forEach((k) => values.push(obj[k]));

        const base = i * COLS.length;
        const ph = COLS.map((_, j) => `$${base + j + 1}`);
        placeholders.push(`(${ph.join(',')})`);
      });

      if (placeholders.length === 0) return;

      // INSERT … ON CONFLICT (company_slug, internal_job_id) DO UPDATE …
      const sql = `
        INSERT INTO jobs (${COLS.join(',')})
        VALUES ${placeholders.join(',')}
        ON CONFLICT (company_slug, internal_job_id)
        DO UPDATE SET
          company_name = EXCLUDED.company_name,
          job_id = EXCLUDED.job_id,
          title = EXCLUDED.title,
          url = EXCLUDED.url,
          location_text = EXCLUDED.location_text,
          departments = EXCLUDED.departments,
          offices = EXCLUDED.offices,
          employment_type = EXCLUDED.employment_type,
          workplace_type = EXCLUDED.workplace_type,
          remote_hint = EXCLUDED.remote_hint,
          shift = EXCLUDED.shift,
          updated_at = EXCLUDED.updated_at,
          requisition_open_date = EXCLUDED.requisition_open_date,
          job_category = EXCLUDED.job_category,
          benefits = EXCLUDED.benefits,
          travel_requirement = EXCLUDED.travel_requirement,
          years_experience = EXCLUDED.years_experience,
          experience_range = EXCLUDED.experience_range,
          contractor_type = EXCLUDED.contractor_type,
          address = EXCLUDED.address,
          city = EXCLUDED.city,
          state = EXCLUDED.state,
          country = EXCLUDED.country,
          latitude = EXCLUDED.latitude,
          longitude = EXCLUDED.longitude,
          salary_currency = EXCLUDED.salary_currency,
          salary_min = EXCLUDED.salary_min,
          salary_max = EXCLUDED.salary_max,
          content_text = EXCLUDED.content_text,
          content_html = EXCLUDED.content_html,
          updated_row_at = NOW()
        RETURNING 1;
      `;

      try {
        const result = await pool.query(sql, values);
        inserted += result.rowCount;
      } catch (err) {
        console.error('NDJSON batch insert error:', err);
        errors.push({ batchError: err.message });
      }
    };

    // chunk + insert
    const current = [];
    for (const line of lines) {
      read++;
      if (!line.trim()) continue;

      try {
        const obj = JSON.parse(line);
        current.push(obj);
      } catch (err) {
        errors.push({ row: read, error: `invalid JSON: ${err.message}` });
        continue;
      }

      if (current.length >= batchSize) {
        await insertBatch(current.splice(0, current.length));
      }
    }
    // tail
    await insertBatch(current);

    res.json({ ok: true, read, inserted, errorsCount: errors.length, errors });
  });

  req.on('error', (e) => {
    console.error('NDJSON stream error:', e);
  });
});

// ---- START ----------------------------------------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`jobs-backend listening on :${PORT}`);
});
