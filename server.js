import express from "express";
import pkg from "pg";
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const app = express();
app.use(express.json());

app.get("/health", async (_req, res) => {
  try {
    const r = await pool.query("select now() as ts");
    res.json({ ok: true, ts: r.rows[0].ts });
  } catch {
    res.status(500).json({ ok: false });
  }
});

app.get("/api/jobs/latest", async (req, res) => {
  try {
    const vertical = req.query.vertical || "AI";
    const cats =
      vertical === "Finance"
        ? ["Finance", "FinTech", "FP&A", "Accounting"]
        : ["AI/ML", "Data", "Machine Learning"];
    const { rows } = await pool.query(
      `SELECT id,title,company_slug,city,state,country,requisition_open_date
         FROM jobs
        WHERE is_active=TRUE AND job_category = ANY($1)
        ORDER BY requisition_open_date DESC NULLS LAST
        LIMIT 20`,
      [cats]
    );
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: "query_failed" });
  }
});

app.listen(process.env.PORT || 3000, () => console.log("Jobs API running"));
