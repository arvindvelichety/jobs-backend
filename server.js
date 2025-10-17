// server.js
import express from 'express'
import cors from 'cors'
import helmet from 'helmet'
import compression from 'compression'
import morgan from 'morgan'
import { Pool } from 'pg'

/**
 * ---- Environment ----
 * Set these in Render (or a local .env if running locally)
 * - DATABASE_URL (Neon Postgres connection string; include sslmode)
 * - ALLOWED_ORIGINS (comma-separated Origins)
 * - PORT (Render provides automatically; locally defaults to 3000)
 * - NODE_ENV (production/development)
 */
const {
  DATABASE_URL,
  PORT = process.env.PORT || 3000,
  ALLOWED_ORIGINS = 'https://ergasiatech.com,https://www.ergasiatech.com,http://localhost:5173',
  NODE_ENV = 'production',
} = process.env

if (!DATABASE_URL) {
  console.error('ERROR: DATABASE_URL not set'); process.exit(1)
}

// ---- DB ----
const pool = new Pool({
  connectionString: DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 10_000,
})

// ---- App ----
const app = express()

app.use(helmet({ contentSecurityPolicy: false }))
app.use(compression())
app.use(express.json({ limit: '1mb' }))
app.use(morgan(NODE_ENV === 'production' ? 'combined' : 'dev'))

// ---- CORS (allowlist) ----
const allowed = ALLOWED_ORIGINS.split(',').map(s => s.trim()).filter(Boolean)
app.use(cors({
  origin: (origin, cb) => {
    if (!origin) return cb(null, true) // allow curl/same-origin
    if (allowed.includes(origin)) return cb(null, true)
    return cb(new Error('Not allowed by CORS: ' + origin))
  },
  methods: ['GET','HEAD','OPTIONS'],
  allowedHeaders: ['Content-Type','Authorization'],
  credentials: false,
}))
app.options('*', cors())

// ---- Helpers ----
const clamp = (n, min, max) => Math.max(min, Math.min(max, n))

/**
 * Build a where clause for a simple ILIKE search across key fields.
 * IMPORTANT: Keep this in sync with the fields you SELECT below.
 */
function buildSearchWhere(search, params) {
  if (!search) return { where: '', params }
  params.push(`%${search}%`)
  const p = params.length

  // Must match the fields you actually have in your DB.
  // If your schema differs, adjust these columns accordingly.
  const locExpr = `COALESCE(location, job_location, NULLIF(TRIM(CONCAT_WS(', ', city, state)), ''), '')`

  const clauses = [
    `title ILIKE $${p}`,
    `company_name ILIKE $${p}`,
    `${locExpr} ILIKE $${p}`,
    `description ILIKE $${p}`,
  ]
  return { where: `WHERE (${clauses.join(' OR ')})`, params }
}

/**
 * Use a single expression to normalize "location"
 * Adjust the list (location, job_location, city, state) to match YOUR schema.
 * If you have e.g. "country" or "region", you can add it to CONCAT_WS.
 */
const LOC_EXPR = `COALESCE(location, job_location, NULLIF(TRIM(CONCAT_WS(', ', city, state)), ''), '')`


// ---- Routes ----

// Health check
app.get('/api/health', async (_req, res) => {
  try {
    const r = await pool.query('SELECT 1 AS ok')
    res.json({ ok: true, db: r.rows[0].ok === 1, env: NODE_ENV })
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message })
  }
})

// GET /api/jobs?search=&limit=&offset=
app.get('/api/jobs', async (req, res) => {
  try {
    const search = (req.query.search || '').toString().trim()
    const limit = clamp(parseInt(req.query.limit || '25', 10) || 25, 1, 100)
    const offset = Math.max(parseInt(req.query.offset || '0', 10) || 0, 0)

    let params = []
    const { where, params: p2 } = buildSearchWhere(search, params)
    params = p2

    // Select and ALIAS to the shape the frontend expects
    const sql = `
      SELECT
        id,
        company_name,
        title,
        ${LOC_EXPR} AS location,
        description,
        url,
        created_at
      FROM jobs
      ${where}
      ORDER BY created_at DESC NULLS LAST, id DESC
      LIMIT $${params.push(limit)}
      OFFSET $${params.push(offset)}
    `
    const { rows } = await pool.query(sql, params)
    res.json({ ok: true, limit, offset, count: rows.length, jobs: rows })
  } catch (e) {
    console.error('GET /api/jobs error:', e)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// GET /api/jobs/:id
app.get('/api/jobs/:id', async (req, res) => {
  try {
    const { id } = req.params
    const { rows } = await pool.query(
      `
      SELECT
        id,
        company_name,
        title,
        ${LOC_EXPR} AS location,
        description,
        url,
        created_at
      FROM jobs
      WHERE id = $1
      LIMIT 1
      `,
      [id]
    )
    if (!rows.length) return res.status(404).json({ ok: false, error: 'Not found' })
    res.json({ ok: true, job: rows[0] })
  } catch (e) {
    console.error('GET /api/jobs/:id error:', e)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// API 404
app.use('/api', (_req, res) => res.status(404).json({ ok: false, error: 'Not found' }))

// Error handler
app.use((err, _req, res, _next) => {
  console.error('Unhandled error:', err)
  res.status(500).json({ ok: false, error: err.message || 'Server error' })
})

// Start
app.listen(PORT, () => {
  console.log(`API listening on ${PORT} (${NODE_ENV})`)
  console.log(`Allowed origins: ${allowed.join(', ')}`)
})
