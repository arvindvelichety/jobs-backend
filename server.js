// server.js
import express from 'express'
import cors from 'cors'
import helmet from 'helmet'
import compression from 'compression'
import morgan from 'morgan'
import { Pool } from 'pg'

/**
 * ---- Config ----
 * Set env vars in Render or a local .env:
 * - DATABASE_URL (Postgres connection string from Neon)
 * - PORT (optional, defaults 10000 on Render or 3000 locally)
 * - ALLOWED_ORIGINS (comma-separated list of allowed Origins)
 * - NODE_ENV (production/development)
 */
const {
  DATABASE_URL,
  PORT = process.env.PORT || 3000,
  ALLOWED_ORIGINS = 'https://ergasiatech.com,https://www.ergasiatech.com,http://localhost:5173',
  NODE_ENV = 'production'
} = process.env

if (!DATABASE_URL) {
  console.error('ERROR: DATABASE_URL not set')
  process.exit(1)
}

const allowedOrigins = ALLOWED_ORIGINS.split(',').map(s => s.trim()).filter(Boolean)

// ---- DB Pool ----
const pool = new Pool({
  connectionString: DATABASE_URL,
  // Neon is fine with defaults; tune if needed:
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 10_000,
})

// ---- App ----
const app = express()

// Security & perf
app.use(helmet({
  contentSecurityPolicy: false, // keep simple; tighten if needed
}))
app.use(compression())
app.use(express.json({ limit: '1mb' }))

// Logging
app.use(morgan(NODE_ENV === 'production' ? 'combined' : 'dev'))

// CORS (allowlist)
app.use(cors({
  origin: (origin, cb) => {
    // allow curl / same-origin requests without Origin
    if (!origin) return cb(null, true)
    if (allowedOrigins.includes(origin)) return cb(null, true)
    return cb(new Error(`Not allowed by CORS: ${origin}`))
  },
  methods: ['GET', 'HEAD', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: false,
}))

// Preflight (some hosts require explicit handler)
app.options('*', cors())

// ---- Helpers ----
const clamp = (n, min, max) => Math.max(min, Math.min(max, n))

function buildSearchWhere(search, params) {
  if (!search) return { where: '', params }
  // Simple ILIKE search across common columns
  params.push(`%${search}%`)
  const p = params.length
  const clauses = [
    `title ILIKE $${p}`,
    `company_name ILIKE $${p}`,
    `location ILIKE $${p}`,
    `description ILIKE $${p}`,
  ]
  return { where: `WHERE (${clauses.join(' OR ')})`, params }
}

// ---- Routes ----

// Health / readiness
app.get('/api/health', async (req, res) => {
  try {
    const r = await pool.query('SELECT 1 as ok')
    res.json({ ok: true, db: r.rows[0].ok === 1, env: NODE_ENV })
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message })
  }
})

// List jobs with search + pagination
// GET /api/jobs?search=&limit=&offset=
app.get('/api/jobs', async (req, res) => {
  try {
    const search = (req.query.search || '').toString().trim()
    const limit = clamp(parseInt(req.query.limit || '25', 10) || 25, 1, 100)
    const offset = Math.max(parseInt(req.query.offset || '0', 10) || 0, 0)

    let params = []
    const { where, params: p2 } = buildSearchWhere(search, params)
    params = p2

    const sql = `
      SELECT id, company_name, title, location, description, url, created_at
      FROM jobs
      ${where}
      ORDER BY created_at DESC NULLS LAST, id DESC
      LIMIT $${params.push(limit)}
      OFFSET $${params.push(offset)}
    `
    const { rows } = await pool.query(sql, params)

    res.json({
      ok: true,
      limit,
      offset,
      count: rows.length,
      jobs: rows,
    })
  } catch (e) {
    console.error('GET /api/jobs error:', e)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// Get one job
// GET /api/jobs/:id
app.get('/api/jobs/:id', async (req, res) => {
  try {
    const { id } = req.params
    const { rows } = await pool.query(
      `SELECT id, company_name, title, location, description, url, created_at
       FROM jobs
       WHERE id = $1
       LIMIT 1`,
      [id]
    )
    if (!rows.length) return res.status(404).json({ ok: false, error: 'Not found' })
    res.json({ ok: true, job: rows[0] })
  } catch (e) {
    console.error('GET /api/jobs/:id error:', e)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// Fallback 404 (API only; do not serve files from here)
app.use('/api', (req, res) => {
  res.status(404).json({ ok: false, error: 'Not found' })
})

// Global error handler
// (Express will pass errors here if next(err) used)
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err)
  res.status(500).json({ ok: false, error: err.message || 'Server error' })
})

// ---- Start ----
app.listen(PORT, () => {
  console.log(`API listening on port ${PORT} in ${NODE_ENV} mode`)
  console.log(`Allowed origins: ${allowedOrigins.join(', ')}`)
})
