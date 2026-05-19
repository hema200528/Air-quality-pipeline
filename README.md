# Air Quality Data Pipeline — Team 4

A multi-week data engineering project working with air-quality sensor
data from Delhi monitoring stations (2024–2025). The dataset was
ingested by the instructor from multiple CSV sources, joined into one
master dataset, and split across 8 teams. This is Team 4's portion.

---

## Dataset Overview

- **Source:** Air-quality monitoring stations in Delhi
- **Time range:** January 2024 – December 2025
- **Size:** 5,831,431 rows × 22 columns
- **Format received:** Parquet (binary columnar format)
- **Stations:** 10 (all in Delhi)
- **Pollutants tracked:** 13
  - 8 standard CPCB pollutants: PM2.5, PM10, NO2, SO2, CO, O3, NH3, benzene
  - 5 additional VOCs: toluene, xylene, mp_xylene, eth_benzene, NO

The instructor handled the raw ingestion (collect → join → save as
parquet). Each team received one `.parquet` file. Team 4's file is
`team_4.parquet`.

---

## Why parquet (and not CSV)?

The dataset arrived as parquet, not CSV. Worth noting why this matters:

- **Size.** 5.8M rows fits in 54 MB as parquet. As CSV the same data
  would be ~700 MB to 1 GB.
- **Speed.** Parquet stores data column-by-column, so reading specific
  columns is much faster than CSV's row-by-row format.
- **Data types are preserved.** Timestamps stay as timestamps, floats
  stay as floats. CSV turns everything into text.
- **Industry standard** for data engineering pipelines.

Trade-off: parquet is binary, so you can't open it in Notepad. You
need a library like pandas + pyarrow to read it.

---

## Week 1 — Ingestion Summary & Partitioning

### Goal

The instructor said data ingestion was already done by him. Our job:

1. Open the parquet file in Python
2. Produce an **ingestion summary** describing what came in
3. **Partition** the dataset year-wise and month-wise so future work
   doesn't have to load all 5.8M rows every time

### What "ingestion" means

Ingestion = bringing raw data into your system in a usable form.
The instructor's pipeline did: collect CSVs → join them → save as
parquet → split per team. The **ingestion summary** is a report
describing the resulting dataset — like a receipt proving the data
loaded correctly and showing what's in it.

### What we did

1. Installed `pandas` (for handling the table) and `pyarrow` (the
   engine that reads parquet).
2. Loaded `team_4.parquet` into a pandas DataFrame.
3. Generated a summary covering: total rows, date range, station
   count, pollutant list, records per year/month, records per
   pollutant, and missing-value counts.
4. **Partitioned** the dataset into 24 files using Hive-style folders.

### What is Hive-style partitioning?

Splitting a big dataset into smaller files organized in folders named
like `column=value`. Instead of one 5.8M-row file, we ended up with:

```
partitioned_data/
  year=2024/
    month=01/data.parquet
    month=02/data.parquet
    ...
  year=2025/
    month=12/data.parquet
```

The folder names *are* the data — pandas, Spark, DuckDB, and Hive all
read the folder name and automatically know "everything inside this
folder belongs to January 2024."

**Why bother?** If someone only needs Jan 2024, they load that one
small file instead of the whole dataset. This is the same pattern used
in production data lakes.

**Gotcha we hit:** First version of the partitioning kept `year` and
`month` columns inside the files AND in the folder names. When reading
the whole partitioned dataset back, pandas threw a type-mismatch error
because the column from the file (int32) didn't match the column from
the folder (dictionary type). Fix: drop `year` and `month` from the
data before saving — the folder names already encode them.

### Observations on data quality

Looking at the missing-value report flagged things for downstream
cleaning:

- `vws_m_s` column is **100% null** — should be dropped entirely
- `ws_m_s`, `sr_w_mt2`, `at_c`, `bp_mmhg`, `rf_mm` are 30–45% null —
  cleaning/imputation needed
- Instructor said "8 main species" but there are 13 pollutants in the
  file. The 8 CPCB criteria pollutants are clear; the other 5 are
  VOCs. Worth confirming whether to filter the dataset down to 8

### Week 1 deliverables

- `week1_ingestion.ipynb` — Colab notebook with all the work
- `ingestion_summary.txt` — text file with the summary
- `partitioned_data/` — 24 partition files *(not pushed to git, too
  large — kept locally)*

---

## Week 2 — Data Model + Database

### Goal

Build a **data model** for the dataset and load it into a database.
Choose between DBMS / NoSQL / wide-column storage. Measure load time
and query time, because "time is money."

### What is a data model?

A plan for how the dataset will live inside a database. Three
decisions:

1. **What tables exist and what columns go in each?**
2. **What data types?** (timestamp, integer, float, string, etc.)
3. **What are the keys and relationships?**

The parquet file is just raw data sitting on disk. A data model turns
it into something a database can store, query, and serve fast.

### Choosing the storage mechanism

The assignment listed three options. Here's how I thought about each:

| Option | Verdict | Reasoning |
|---|---|---|
| **DBMS** (Postgres, MySQL, SQLite) | Good fit | Data is highly structured — fixed columns, fixed types. Classic relational DBs handle this well. |
| **NoSQL** (MongoDB, Cassandra) | Bad fit | NoSQL's strength is flexible/unstructured data. Our data has a strict schema, so the flexibility is wasted and we lose SQL's analytical power. |
| **Wide-column / columnar** (DuckDB, ClickHouse, BigQuery) | Best fit | Built for analytics on time-series data. Stores data column-by-column, just like parquet. |

**Picked DuckDB** because:

- It's a columnar analytical database — same storage philosophy as
  parquet, so queries on millions of rows are fast.
- Zero server setup — runs entirely inside Python. The whole database
  is one file.
- It reads parquet files directly, so loading is easy.
- It speaks standard SQL.
- It's the right size of tool for this dataset — Postgres would work
  but is heavier; ClickHouse/BigQuery are overkill for 5.8M rows.

### The schema — star schema

Used a **star schema**: one big fact table + small dimension tables.
This is the standard pattern in data warehousing.

```
              ┌──────────────┐
              │  stations    │  ← 10 rows
              │ station_id PK│
              │ city, state  │
              └──────┬───────┘
                     │
                     ▼
              ┌────────────────────┐
              │   measurements     │  ← 5.8M rows
              │ measurement_id     │
              │ datetime           │
              │ station_id   FK ───┘
              │ pollutant_id FK ───┐
              │ value              │
              │ year, month, ...   │
              │ at_c, rh_percent,  │
              │ bp_mmhg, ws_m_s,   │
              │ wd_deg, rf_mm,     │
              │ sr_w_mt2, vws_m_s  │
              └────────────────────┘
                     ▲
              ┌──────┴────────┐
              │  pollutants   │  ← 13 rows
              │ pollutant_id  │
              │ pollutant_name│
              └───────────────┘
```

**Why this design?**

- **Normalization.** "Delhi" doesn't get stored 5.8 million times —
  it's stored once in `stations` and referenced by `station_id`. Same
  for pollutant names.
- **Smaller fact table** = faster scans.
- **Joins on small dimension tables are cheap.** The pollutants table
  is only 13 rows; joining it costs nothing.
- **Standard pattern** that any data engineer would recognize.

### Step-by-step process (week 2)

1. **Install DuckDB** in Colab.
2. **Connect** with `duckdb.connect("air_quality.duckdb")` —
   this creates the database file. Unlike Postgres/MySQL, there's no
   server. The database is just a file.
3. **Inspect the parquet's schema** with `DESCRIBE SELECT * FROM
   'team_4.parquet'` to see column names and types before designing
   the schema. (DuckDB can read parquet without importing it — useful.)
4. **Create three empty tables** with explicit column types and
   primary keys.
5. **Populate the dimension tables** — pulled the 10 distinct stations
   and 13 distinct pollutants from the parquet using `SELECT DISTINCT`.
   Assigned pollutants integer IDs (1–13) with `ROW_NUMBER()`.
6. **Populate the fact table** — inserted 5.8M rows from the parquet,
   joining on pollutant name to replace it with the integer ID.
   **Timed this step.**
7. **Ran four benchmark queries** with timing wrappers — typical
   questions someone analyzing air quality might actually ask.
8. **Compared** the database query time to running the same query
   directly on the parquet file (no DB involved). DuckDB can do both.
9. **Measured** the final database file size.

### Benchmark results — "time is money"

Run on Google Colab (free tier), May 2026.

| Operation | Result |
|---|---|
| **Load 5.8M rows into DuckDB** | 11.70 seconds |
| Query: avg PM2.5 per month | 37.8 ms (24 rows) |
| Query: top 3 stations by avg PM10 | 82.5 ms (3 rows) |
| Query: measurement count per pollutant | 126.9 ms (13 rows) |
| Query: daily peak NO2 for one station | 56.7 ms (716 rows) |
| Same Q1 against parquet directly (no DB) | 63.4 ms |
| Database file size | 80.5 MB |
| Original parquet size | 54 MB |

**Takeaways:**

- Loading was fast — 5.8M rows in under 12 seconds.
- All four queries finished in **under 130 milliseconds**. For
  comparison, a human blink is ~100ms. Fast enough that the database
  could power a real-time dashboard.
- The DB is ~50% bigger than the parquet (80.5 MB vs 54 MB), which is
  the price of having indexes and a proper schema. Worth it for the
  query speeds.
- Interesting — querying the parquet directly (63.4 ms) was actually
  slower than querying the loaded DB (37.8 ms). So loading is worth it
  for repeated queries; for one-off queries the direct-parquet path
  saves the 12-second load step. Real-world tradeoff.

### Week 2 deliverables

- `week2_data_model.ipynb` — Colab notebook
- `air_quality.duckdb` — the database file *(not pushed to git, kept
  locally)*

---

## How to reproduce

1. Install dependencies:
   ```
   pip install pandas pyarrow duckdb
   ```
2. Place `team_4.parquet` in the project root (not included in repo —
   too large).
3. Open `week1_ingestion.ipynb` and run cells in order to produce the
   partitioned data and summary.
4. Open `week2_data_model.ipynb` and run cells in order to build the
   database and run benchmarks.

---

## Project structure

```
DT_project/
├── README.md                       ← this file
├── .gitignore                      ← keeps data files out of git
├── week1_ingestion.ipynb           ← week 1 notebook
├── week2_data_model.ipynb          ← week 2 notebook
├── ingestion_summary.txt           ← week 1 output
├── team_4.parquet                  ← source data (gitignored)
├── partitioned_data/               ← week 1 output (gitignored)
│   └── year=YYYY/month=MM/data.parquet
└── air_quality.duckdb              ← week 2 output (gitignored)
```

---

## Things I learned doing this

- Parquet's columnar layout isn't just an implementation detail —
  it's why DuckDB queries are so fast on it.
- Hive-style partitioning seems weird at first (folder names as data?)
  but is genuinely useful — and standard.
- Star schema with one fact table and small dimension tables really
  does make queries cleaner to write.
- Loading into a proper database isn't always necessary — DuckDB can
  query parquet directly. The tradeoff is load time vs query time:
  load once, query many → use the DB; one-off → query the parquet.
- The `ROW_NUMBER() OVER ()` trick for generating IDs is a SQL pattern
  I hadn't seen before.

---

## TODO

- Confirm with instructor whether to filter the dataset down to the 8
  CPCB criteria pollutants only, or keep all 13.
- Future weeks: data cleaning (handle the missing weather columns,
  drop `vws_m_s`).
