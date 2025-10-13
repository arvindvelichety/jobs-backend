// server.js
// Minimal jobs backend with CSV import → Neon (Postgres)
// Works on Render + local dev

import express from 'express';
import fetch from 'node-fetch';
import { Pool } from 'pg';
import { parse } from 'csv-parse';

// Load .env locally (Render will already have env vars)
try { (await import('dotenv')).config(); } catch (_) {}

const app = express();

// --- Env & DB ---------------------------------------------------------------

const PORT = process.env.PORT || 10000;

// Required: DATABASE_URL (from Neon) and IMPORT_TOKEN (your secret)
const DATABASE_URL = process.env.DATABASE_URL;
const IMPORT_TOKEN  = process.env.IMPORT_TOKEN;

if (!DATABASE_URL) {
  console.error('ERROR: DATABASE_URL is not set');
}
if (!IMPORT_TOKEN) {
  console.warn('WARNING: IMPORT_TOKEN is not set — /api/import will always 401');
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }, // Render + Neon
});

// Simple DB check helper
async function dbOk() {
  try {
    const r = await pool.query('select 1 as ok');
    return r.rows[0]?.ok === 1;
  } catch (e) {
    console.error('DB health error:', e.message);
    return false;
  }
}

// --- CSV helpers ------------------------------------------------------------

const csvOptions = {
  columns: true,        // header row -> keys
  bom: true,            // handle UTF-8 BOM
  trim: true,           // trim whitespace
  skip_empty_lines: true,
};

const get = (row, key) => {
  const v = (row?.[key] ?? '').toString().trim();
  return v === '' ? null : v;
};

// Map one CSV row to DB columns (names must match your table)
const mapRow = (row) => ({
  company_slug: get(row, 'company_slug'),
  company_name: get(row, 'company_name'),
  job_id: get(row, 'job_id'),
  internal_job_id: get(row, 'internal_job_id') ?? get(row, 'job_id'),
  title: get(row, 'title'),
  url: get(row, 'url'),
  location_text: get(row, 'location_text'),
  departments: get(row, 'departments'),
  offices: get(row, 'offices'),
  employment_type: get(row, 'employment_type'),
  workplace_type: get(row, 'workplace_type'),
  remote_hint: get(row, 'remote_hint'),
  shift: get(row, 'shift'),
  updated_at: get(row, 'updated_at'),
  requisition_open_date: get(row, 'requisition_open_date'),
  job_category: get(row, 'job_category'),
  benefits: get(row, 'benefits'),
  travel_requirement: get(row, 'travel_requirement'),
  years_experience: get(row, 'years_experience'),
  experience_range: get(row, 'experience_range'),
  contractor_type: get(row, 'contractor_type'),
  address: get(row, 'address'),
  city: get(row, 'city'),
  state: get(row, 'state'),
  country: get(row, 'country'),
  latitude: get(row, 'latitude'),
  longitude: get(row, 'longitude'),
  salary_currency: get(row, 'salary_currency'),
  salary_min: get(row, 'salary_min'),
  salary_max: get(row, 'salary_max'),
  content_text: get(row, 'content_text'),
  content_html: get(row, 'content_html'),
});

// Column order we’ll insert in
const COLS = [
  'company_slug','company_name','job_id','internal_job_id','title','url',
  'location_text','departments','offices','employment_type','workplace_type',
  'remote_hint','shift','updated_at','requisition_open_date','job_category',
  'benefits','travel_requirement','years_experience','experience_range',
  'contractor_type','address','city','state','country','latitude','longitude',
  'salary_currency','salary_min','salary_max','content_text','content_html'
];

// Build a parameter list like $1,$2,...,$32
const PLACEHOLDERS = COLS.map((_, i) => `$${i + 1}`).join(',');

// Prepared insert that ignores duplicates (whatever unique you’ve set)
const INSERT_SQL = `
INSERT INTO jobs (${COLS.join(',')})
VALUES (${PLACEHOLDERS})
ON CONFLICT DO NOTHING
`;

// --- Routes -----------------------------------------------------------------

// Root (simple JSON so Render doesn’t 502)
app.get('/', (req, res) => {
  res.json({ ok: true, service: 'jobs-backend', env: process.env.NODE_ENV || 'development' });
});

// Old health path Render likes
app.get('/healthz', async (req, res) => {
  res.type('text').send('ok');
});

// API health (checks DB)
app.get('/api/health', async (req, res) => {
  res.json({ ok: true, db: (await dbOk()) ? 'ok' : 'down', ts: new Date().toISOString() });
});

// Count jobs
app.get('/api/jobs/count', async (req, res) => {
  try {
    const r = await pool.query('select count(*)::int as count from jobs');
    res.json({ ok: true, count: r.rows[0].count });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Import CSV → jobs
// Usage:
// curl -X POST "https://.../api/import?url=ENCODED_CSV_URL&dryRun=false" -H "x-import-token: YOURTOKEN"
app.post('/api/import', async (req, res) => {
  try {
    // Token check
    const incoming = req.header('x-import-token') || '';
    if (!IMPORT_TOKEN || incoming !== IMPORT_TOKEN) {
      return res.status(401).json({ ok: false, error: 'unauthorized (bad x-import-token)' });
    }

    const csvUrl = req.query.url?.toString();
    if (!csvUrl) {
      return res.status(400).json({ ok: false, error: 'missing ?url=' });
    }

    const dryRun = (req.query.dryRun ?? 'true').toString().toLowerCase() !== 'false';

    // Fetch CSV
    const resp = await fetch(csvUrl);
    if (!resp.ok) {
      return res.status(400).json({ ok: false, error: `fetch failed: ${resp.status} ${resp.statusText}` });
    }
    const body = await resp.text();

    // Parse CSV to objects
    const rows = await new Promise((resolve, reject) => {
      const out = [];
      parse(body, csvOptions)
        .on('readable', function () {
          let rec;
          while ((rec = this.read()) !== null) out.push(rec);
        })
        .on('error', reject)
        .on('end', () => resolve(out));
    });

    if (rows.length === 0) {
      return res.json({ ok: true, parsed: 0, previewCount: 0, preview: [], insert: dryRun ? 'skipped' : 'none' });
    }

    const mapped = rows.map(mapRow);

    // Basic validations for NOT NULL columns you mentioned
    const requiredFields = ['company_slug', 'internal_job_id'];
    const missing = [];
    mapped.forEach((m, idx) => {
      requiredFields.forEach((f) => {
        if (m[f] == null) missing.push({ row: idx + 1, field: f });
      });
    });
    if (missing.length) {
      return res.status(400).json({ ok: false, error: 'required fields missing', details: missing.slice(0, 10) });
    }

    // Dry run?
    if (dryRun) {
      return res.json({
        ok: true,
        parsed: mapped.length,
        previewCount: Math.min(mapped.length, 3),
        preview: mapped.slice(0, 3),
        insert: 'skipped',
      });
    }

    // Insert in a transaction
    const client = await pool.connect();
    let inserted = 0;
    try {
      await client.query('BEGIN');
      for (const m of mapped) {
        const values = COLS.map((c) => m[c] ?? null);
        await client.query(INSERT_SQL, values);
        inserted += 1; // counts attempted inserts; conflicts are ignored by DO NOTHING
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    // Return count + a tiny preview
    res.json({
      ok: true,
      parsed: mapped.length,
      inserted,
      previewCount: Math.min(mapped.length, 2),
      preview: mapped.slice(0, 2),
    });
  } catch (e) {
    console.error('IMPORT ERROR:', e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --- Start ------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`jobs-backend listening on :${PORT}`);
});
