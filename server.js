// server.js
import "dotenv/config";
import express from "express";
import fetch from "node-fetch";
import { Pool } from "pg";
import { parse as csvParse } from "csv-parse";
import readline from "node:readline";

// ---------- config ----------
const app = express();
app.disable("x-powered-by");

const PORT = process.env.PORT || 3000;
const IMPORT_TOKEN = (process.env.IMPORT_TOKEN || "").trim();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Render + Neon typically require ssl in prod
  ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false,
});

// ---------- small utils ----------
const ok = (res, data = {}) => res.json({ ok: true, ...data });
const err = (res, status, message, extra = {}) =>
  res.status(status).json({ ok: false, error: message, ...extra });

/** basic auth guard using x-import-token */
const checkToken = (req, res) => {
  const t = (req.headers["x-import-token"] || "").trim();
  if (!IMPORT_TOKEN || !t || t !== IMPORT_TOKEN) {
    err(res, 401, "unauthorized");
    return false;
  }
  return true;
};

// ---------- DB schema discovery (one-time cached) ----------
let JOBS_SCHEMA = null;
/**
 * Loads public.jobs columns/types so we only write fields that really exist.
 * Result shape: { columns: Set<string>, types: Map<string, string> }
 */
async function loadJobsSchema() {
  if (JOBS_SCHEMA) return JOBS_SCHEMA;
  const sql = `
    select column_name, data_type, udt_name
    from information_schema.columns
    where table_schema='public' and table_name='jobs'
  `;
  const { rows } = await pool.query(sql);
  const columns = new Set(rows.map(r => r.column_name));
  const types = new Map(rows.map(r => [r.column_name, (r.udt_name || r.data_type || "").toLowerCase()]));
  JOBS_SCHEMA = { columns, types };
  return JOBS_SCHEMA;
}

// ---------- value normalization ----------
/**
 * Convert an incoming value to something Postgres accepts based on column type.
 * We’re conservative: strings remain strings; numeric -> Number; json/jsonb -> JSON; others pass-through.
 */
function coerceForColumn(col, val, types) {
  if (val === undefined || val === null || val === "") return null;
  const t = (types.get(col) || "").toLowerCase();

  // json/jsonb columns
  if (t === "json" || t === "jsonb") {
    if (typeof val === "object") return val; // already array/object
    const s = String(val).trim();
    if (!s) return null;
    try {
      // if it's already JSON text
      if (s.startsWith("{") || s.startsWith("["))
        return JSON.parse(s);
      // treat pipe-delimited as array (common in your CSV)
      if (s.includes("|"))
        return s.split("|").map(x => x.trim()).filter(Boolean);
      // plain string -> store as JSON string
      return s;
    } catch {
      // fallback to string if parsing failed
      return String(val);
    }
  }

  // numeric-ish
  if (["int4","int8","float4","float8","numeric","decimal"].some(k => t.includes(k))) {
    const n = Number(val);
    return Number.isFinite(n) ? n : null;
  }

  // boolean
  if (t === "bool") {
    const s = String(val).toLowerCase();
    if (["true","t","1","yes","y"].includes(s)) return true;
    if (["false","f","0","no","n"].includes(s)) return false;
    return null;
  }

  // timestamps/dates: let Postgres cast strings
  return String(val);
}

/**
 * Build a parameterized INSERT for one row.
 * Uses "ON CONFLICT DO NOTHING" so we don’t need to know the constraint/index name.
 */
function buildInsert(table, row, columns, types) {
  const cols = [];
  const params = [];
  const values = [];

  for (const col of columns) {
    if (!(col in row)) continue;
    cols.push(col);
    params.push(`$${params.length + 1}`);
    values.push(coerceForColumn(col, row[col], types));
  }

  if (cols.length === 0) return null;

  const sql = `
    insert into ${table} (${cols.map(c => `"${c}"`).join(", ")})
    values (${params.join(", ")})
    on conflict do nothing
  `;
  return { sql, values };
}

/**
 * Upsert many rows (row-by-row inside one transaction; simple and robust).
 * Returns { ok: number, errors: Array<{row:number,error:string}> }
 */
async function upsertMany(rows, client) {
  const { columns, types } = await loadJobsSchema();

  // Only insert columns that exist in the table, intersected with the incoming keys for each row
  let okCount = 0;
  const errors = [];

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i] || {};
    // keep all keys but filter against actual columns
    const cols = Object.keys(r).filter(k => columns.has(k));

    const stmt = buildInsert("public.jobs", r, cols, types);
    if (!stmt) continue;

    try {
      await client.query(stmt.sql, stmt.values);
      okCount++;
    } catch (e) {
      errors.push({ row: i + 1, error: e.message });
    }
  }

  return { ok: okCount, errors };
}

// ---------- CSV importer (existing /api/import) ----------
app.post("/api/import", async (req, res) => {
  try {
    if (!checkToken(req, res)) return;

    const url = String(req.query.url || "").trim();
    if (!url) return err(res, 400, "missing url");

    const dryRun = String(req.query.dryRun || "true").toLowerCase() !== "false";

    // fetch remote file
    const r = await fetch(url, { redirect: "follow" });
    if (!r.ok) return err(res, 400, `failed to fetch: ${r.status} ${r.statusText}`);

    // stream parse CSV -> objects
    const parser = r.body.pipe(
      csvParse({
        columns: true,
        bom: true,
        trim: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
      })
    );

    const rows = [];
    for await (const rec of parser) {
      // normalize: empty strings -> null
      for (const k of Object.keys(rec)) {
        if (rec[k] === "") rec[k] = null;
        if (typeof rec[k] === "string") rec[k] = rec[k].trim();
      }
      rows.push(rec);
    }

    if (dryRun) {
      return ok(res, {
        parsed: rows.length,
        previewCount: Math.min(2, rows.length),
        preview: rows.slice(0, 2),
        insert: "skipped",
      });
    }

    const client = await pool.connect();
    try {
      await client.query("begin");
      const { ok: inserted, errors } = await upsertMany(rows, client);
      await client.query("commit");
      return ok(res, { parsed: rows.length, inserted, errors });
    } catch (e) {
      await client.query("rollback");
      return err(res, 500, e.message);
    } finally {
      client.release();
    }
  } catch (e) {
    return err(res, 500, e.message);
  }
});

// ---------- NDJSON importer (recommended) ----------
app.post("/api/jobs/ndjson", async (req, res) => {
  try {
    if (!checkToken(req, res)) return;

    const BATCH = Math.max(1, Math.min(5000, Number(req.query.batch || 1000)));
    const dryRun = String(req.query.dryRun || "false").toLowerCase() === "true";
    const rl = readline.createInterface({ input: req });

    let batch = [];
    let total = 0;
    const errors = [];

    const client = await pool.connect();
    const flush = async () => {
      if (!batch.length || dryRun) {
        total += batch.length;
        batch = [];
        return;
      }
      const { ok: inserted, errors: errs } = await upsertMany(batch, client);
      total += inserted;
      if (errs?.length) errors.push(...errs);
      batch = [];
    };

    try {
      await client.query("begin");

      for await (const line of rl) {
        const s = line.trim();
        if (!s) continue;
        let obj;
        try {
          obj = JSON.parse(s);
        } catch (e) {
          errors.push({ error: "bad JSON line", message: e.message, line: s.slice(0, 120) });
          continue;
        }
        // light normalization
        for (const k of Object.keys(obj)) {
          if (obj[k] === "") obj[k] = null;
          if (typeof obj[k] === "string") obj[k] = obj[k].trim();
        }
        batch.push(obj);
        if (batch.length >= BATCH) await flush();
      }

      await flush();

      if (!dryRun) await client.query("commit");
      else await client.query("rollback"); // no writes in dryRun

      return ok(res, { processed: total, errors, dryRun });
    } catch (e) {
      await client.query("rollback");
      return err(res, 500, e.message);
    } finally {
      client.release();
    }
  } catch (e) {
    return err(res, 500, e.message);
  }
});

// ---------- health & small helpers ----------
app.get("/", (_req, res) => res.type("text/plain").send("ok"));
app.get("/healthz", (_req, res) => ok(res, { env: process.env.NODE_ENV || "dev" }));
app.get("/api/health", async (_req, res) => {
  try {
    await pool.query("select 1");
    ok(res, { db: "ok", ts: new Date().toISOString() });
  } catch (e) {
    err(res, 500, e.message);
  }
});
app.get("/api/jobs/count", async (_req, res) => {
  try {
    const { rows } = await pool.query('select count(*)::int as count from public.jobs');
    ok(res, { count: rows[0]?.count ?? 0 });
  } catch (e) {
    err(res, 500, e.message);
  }
});

// ---------- start ----------
app.listen(PORT, () => {
  console.log(`listening on :${PORT}`);
});
