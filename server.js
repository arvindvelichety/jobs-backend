// server.js
import express from "express";
import pkg from "pg";
import dotenv from "dotenv";

dotenv.config();
const { Pool } = pkg;
const app = express();

// database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// simple health check
app.get("/api/health", async (req, res) => {
  try {
    const { rows } = await pool.query("SELECT now() AS ts");
    res.json({ ok: true, db: "ok", ts: rows[0].ts });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// count endpoint
app.get("/api/jobs/count", async (req, res) => {
  try {
    const { rows } = await pool.query("SELECT COUNT(*) AS count FROM jobs");
    res.json({ ok: true, count: parseInt(rows[0].count, 10) });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// list jobs
app.get("/api/jobs", async (req, res) => {
  const limit = parseInt(req.query.limit || "20", 10);
  const offset = parseInt(req.query.offset || "0", 10);
  try {
    const { rows } = await pool.query(
      "SELECT * FROM jobs ORDER BY updated_at DESC LIMIT $1 OFFSET $2",
      [limit, offset]
    );
    res.json({ ok: true, limit, offset, rows: rows.length, jobs: rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ✅ NDJSON bulk insert route (the one you pasted)
app.post("/api/jobs/ndjson", async (req, res) => {
  try {
    const importToken = req.get("x-import-token");
    if (!process.env.IMPORT_TOKEN || importToken !== process.env.IMPORT_TOKEN) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    // read raw NDJSON body
    let body = "";
    await new Promise((resolve, reject) => {
      req.setEncoding("utf8");
      req.on("data", chunk => (body += chunk));
      req.on("end", resolve);
      req.on("error", reject);
    });

    const lines = body.split(/\r?\n/).filter(Boolean);
    const rows = [];
    for (const line of lines) {
      const o = JSON.parse(line);
      const j = v => (v == null ? null : JSON.stringify(v));
      const txt = v => (v == null ? null : String(v));
      const num = v => (v == null ? null : Number(v));
      const when = v => (v ? new Date(v) : null);

      rows.push({
        company_slug: txt(o.company_slug),
        company_name: txt(o.company_name),
        job_id: txt(o.job_id),
        internal_job_id: txt(o.internal_job_id),
        title: txt(o.title),
        url: txt(o.url),
        location_text: txt(o.location_text),
        departments: j(o.departments),
        offices: j(o.offices),
        employment_type: txt(o.employment_type),
        workplace_type: txt(o.workplace_type),
        remote_hint: txt(o.remote_hint),
        shift: txt(o.shift),
        updated_at: when(o.updated_at),
        requisition_open_date: when(o.requisition_open_date),
        job_category: txt(o.job_category),
        benefits: j(o.benefits),
        travel_requirement: txt(o.travel_requirement),
        years_experience: num(o.years_experience),
        experience_range: txt(o.experience_range),
        contractor_type: txt(o.contractor_type),
        address: txt(o.address),
        city: txt(o.city),
        state: txt(o.state),
        country: txt(o.country),
        latitude: num(o.latitude),
        longitude: num(o.longitude),
        salary_currency: txt(o.salary_currency),
        salary_min: num(o.salary_min),
        salary_max: num(o.salary_max),
        content_text: txt(o.content_text),
        content_html: txt(o.content_html),
      });
    }

    if (rows.length === 0)
      return res.json({ ok: true, read: 0, inserted: 0, errorsCount: 0 });

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const cols = Object.keys(rows[0]);
      const placeholders = [];
      const values = [];
      let p = 1;
      for (const r of rows) {
        const tuple = cols.map(k => `$${p++}`);
        values.push(...cols.map(k => r[k]));
        placeholders.push(`(${tuple.join(",")})`);
      }

      const sql = `
        INSERT INTO jobs (${cols.join(",")})
        VALUES ${placeholders.join(",")}
        ON CONFLICT (company_slug, internal_job_id)
        DO UPDATE SET
          title = EXCLUDED.title,
          url = EXCLUDED.url,
          updated_at = EXCLUDED.updated_at
      `;

      const result = await client.query(sql, values);
      await client.query("COMMIT");

      res.json({ ok: true, read: rows.length, inserted: result.rowCount });
    } catch (e) {
      await client.query("ROLLBACK");
      res.status(400).json({ ok: false, error: e.message });
    } finally {
      client.release();
    }
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// start server
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));
