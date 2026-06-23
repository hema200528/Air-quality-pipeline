# Air Quality Data Pipeline — Team 4

End-to-end data engineering project on Delhi air-quality sensor data (2024–2025).
Built across 4 weeks: ingestion → modelling → cleaning/transformation → serving.

The instructor ingested raw CSVs from 8 source teams, joined them into one master
dataset, and split across 8 teams. This is Team 4's pipeline from that point forward.

---

## Dataset

| Property | Value |
|---|---|
| Source | CPCB air-quality monitoring stations, Delhi |
| Time range | January 2024 – December 2025 |
| Rows | 5,831,431 |
| Columns | 22 |
| Total cells | 128,291,482 |
| File format received | Parquet (binary columnar) |
| File size | 51.8 MB |
| Stations | 10 (all in Delhi) |
| Pollutants | 13 |

**Standard CPCB pollutants (8):** PM2.5, PM10, NO2, SO2, CO, O3, NH3, benzene

**VOCs (5):** toluene, xylene, mp-xylene, ethylbenzene, NO

**Weather columns:** temperature (°C), humidity (%), wind speed/direction, rainfall, solar radiation, barometric pressure, vertical wind speed

---

## Repository structure

```
DT_project/
├── README.md
├── .gitignore
├── week1_ingestion.ipynb        ← Week 1: load, summarise, partition
├── air_quality_duckdb.ipynb     ← Week 2: data model, DuckDB, benchmarks
├── week3_cleaning_eda.ipynb     ← Week 3+4: EDA, clean, transform, serve
├── output/
│   └── ingestion_summary.txt
├── missing_before.png           ← missing values chart before cleaning
├── missing_after.png            ← missing values chart after cleaning
├── team_4.parquet               ← source data (gitignored)
├── partitioned_data/            ← 24 partitions (gitignored)
├── air_quality.duckdb           ← raw database (gitignored)
└── air_quality_served.duckdb   ← clean served database (gitignored)
```

---

## Week 1 — Ingestion & Partitioning

### What ingestion means
Ingestion = bringing raw data into your system in a usable form. The instructor
handled the raw stage: collect CSVs → join → save as parquet → split per team.
Our job was to load the file, summarise it, and partition it for downstream use.

### Why parquet (not CSV)

| Property | CSV | Parquet |
|---|---|---|
| Size (this dataset) | ~700 MB estimated | **51.8 MB** |
| Storage layout | Row by row | Column by column |
| Data types | Lost (everything becomes text) | Preserved |
| Read speed | Slow (reads all columns) | Fast (reads only needed columns) |
| Human readable | Yes | No (binary) |

### Ingestion summary

| Metric | Value |
|---|---|
| Total rows | 5,831,431 |
| Total columns | 22 |
| Date range | 2024-01-01 → 2025-12-31 |
| Stations | 10 |
| Pollutants | 13 |
| Missing cells | 19,402,433 (15.1% of all cells) |
| Duplicate rows | 0 |

### Missing values found (before any cleaning)

| Column | Missing count | Missing % | Issue |
|---|---|---|---|
| `vws_m_s` | 5,831,431 | **100%** | Completely empty — drop |
| `ws_m_s` | 2,540,853 | 43.57% | Needs imputation |
| `sr_w_mt2` | 2,213,776 | 37.96% | Needs imputation |
| `at_c` | 1,990,984 | 34.14% | Needs imputation |
| `bp_mmhg` | 1,950,992 | 33.46% | Needs imputation |
| `rf_mm` | 1,941,815 | 33.30% | Needs imputation |
| `wd_deg` | 1,529,016 | 26.22% | Needs imputation |
| `rh_percent` | 1,403,566 | 24.07% | Needs imputation |

### Hive-style partitioning

Split the dataset into 24 smaller files — one per year-month combination:

```
partitioned_data/
  year=2024/
    month=01/data.parquet   ← 226,196 rows
    month=02/data.parquet   ← 221,832 rows
    ...
  year=2025/
    month=12/data.parquet   ← 245,183 rows
```

**Why partition?** Instead of loading 5.8M rows every time, downstream code loads
only the month it needs. Folder names encode the partition key — pandas, Spark,
DuckDB, and Hive all read this layout natively.

**Gotcha hit:** First version kept `year` and `month` columns inside the files AND
in folder names. Reading back threw a type-mismatch (int32 vs dictionary type).
Fix: drop those columns before saving — folder names already encode them.

### Week 1 deliverables

| File | Description |
|---|---|
| `week1_ingestion.ipynb` | Full notebook |
| `output/ingestion_summary.txt` | Summary report |
| `partitioned_data/` | 24 partition files (local only) |

---

## Week 2 — Data Model & Database

### Storage mechanism decision

| Option | Verdict | Reason |
|---|---|---|
| DBMS — PostgreSQL, SQLite | Partial fit | Row-based; slower on analytical queries |
| NoSQL — MongoDB | Poor fit | Built for unstructured data; ours has strict fixed schema |
| Columnar — DuckDB, ClickHouse | ✅ Best fit | Column-by-column storage matches parquet; built for analytics |

**Chose DuckDB** because: zero server setup, runs inside Python, reads parquet
natively, standard SQL, right-sized for 5.8M rows. One file = entire database.

### Schema — star schema

```
     ┌──────────────┐              ┌───────────────┐
     │   stations   │              │  pollutants   │
     │  (10 rows)   │              │  (13 rows)    │
     ├──────────────┤              ├───────────────┤
     │ 🔑 station_id│              │ 🔑 pollutant_id│
     │ city         │              │ pollutant_name│
     │ state        │              └──────┬────────┘
     └──────┬───────┘                     │
            │ 1                         1 │
            │ ∞                         ∞ │
            ▼                             ▼
     ┌────────────────────────────────────────────┐
     │              measurements                  │
     │              (5,831,431 rows)              │
     ├────────────────────────────────────────────┤
     │ 🔑 measurement_id   BIGINT                 │
     │ 🔗 station_id       → stations             │
     │ 🔗 pollutant_id     → pollutants           │
     │    datetime         TIMESTAMP              │
     │    value            DOUBLE                 │
     │    year, month, day, hour  INTEGER         │
     │    at_c, rh_percent, bp_mmhg  DOUBLE       │
     │    ws_m_s, wd_deg, rf_mm, sr_w_mt2  DOUBLE │
     └────────────────────────────────────────────┘
```

**Why star schema?** "Delhi" is stored 10 times (once per station) instead of
5,831,431 times. "pm25" is stored once instead of ~628,000 times. Joins on
small dimension tables (10 and 13 rows) cost almost nothing.

### Load benchmark

| Metric | Value |
|---|---|
| Rows loaded | 5,831,431 |
| Load time | 11.70 seconds |
| Database file size | 80.5 MB |
| Original parquet size | 51.8 MB |
| Size overhead | +55% (price of indexes and schema) |

### Query benchmarks (DuckDB)

| Query | Result | Time |
|---|---|---|
| Avg PM2.5 per month | 24 rows | 37.8 ms |
| Top 3 stations by avg PM10 | 3 rows | 82.5 ms |
| Measurement count per pollutant | 13 rows | 126.9 ms |
| Daily peak NO2 for one station | 716 rows | 56.7 ms |
| Same Q1 on parquet directly (no DB) | 24 rows | 63.4 ms |

All queries on 5.8M rows completed under 130ms — fast enough for a real-time dashboard.

### Empirical proof — DuckDB vs SQLite

Same dataset, same query (avg PM2.5 per month), two databases:

| Metric | SQLite (row-based) | DuckDB (columnar) | Parquet direct |
|---|---|---|---|
| Load time | 71.51 seconds | 11.70 seconds | n/a |
| File size | 1,078.3 MB | 80.5 MB | 51.8 MB |
| Q1: Avg PM2.5/month | 1,852.7 ms | 86.8 ms | 63.4 ms |
| Q2: Count/pollutant | 7,051.7 ms | 164.4 ms | 51.9 ms |
| **Speed vs SQLite** | baseline | **21–43× faster** | 16–136× faster |
| **Size vs SQLite** | baseline | **13.4× smaller** | 20.8× smaller |

**Why DuckDB wins:** SQLite reads every row whole — all 22 columns — even when
the query only needs 2. DuckDB reads only the columns the query needs and skips
the rest. On Q2 (count by pollutant), only 1 column out of 22 is needed —
DuckDB skips 95% of the data. That gap shows in the numbers.

---

## Week 3 — EDA, Cleaning & Transformation

### EDA findings

| Metric | Value |
|---|---|
| Min pollution value | 0.00 µg/m³ |
| Mean pollution value | 51.48 µg/m³ |
| Max pollution value | 1,000.00 µg/m³ |
| Negative readings | 0 |
| Duplicate rows | 0 |
| WHO annual PM2.5 safe limit | 5 µg/m³ |
| Delhi's cleanest month | August 2024 — avg 28.1 µg/m³ (5.6× over WHO limit) |
| Delhi's worst month | November 2024 — avg 250.4 µg/m³ (50× over WHO limit) |

### Dirty rows — what was found

**Sample of rows with missing weather data (1,990,984 affected rows):**

```
datetime                   station_id  pollutant  value   at_c  ws_m_s  rh_percent
2024-01-01 00:00:00+00:00  site_103    no2        19.40   NaN   NaN     NaN
2024-01-01 00:00:00+00:00  site_118    benzene     0.22   NaN   0.72    93.12
2024-01-01 00:00:00+00:00  site_118    pm10      248.70   NaN   0.72    93.12
```

**100% null column — vws_m_s (all 5,831,431 rows empty):**

```
datetime                   station_id  vws_m_s
2024-01-01 00:00:00+00:00  site_5024   NaN
2024-01-01 00:00:00+00:00  site_103    NaN
```

### Cleaning steps and results

| Step | Action | Values affected |
|---|---|---|
| Drop `vws_m_s` | 100% null — useless column | 1 column removed |
| Drop `timestamp` | Duplicate of `datetime` as plain text | 1 column removed |
| Drop `station` | Duplicate of `station_id` | 1 column removed |
| Remove negatives | Physically impossible readings | 0 found |
| Remove duplicates | Exact duplicate rows | 0 found |
| Fill `at_c` | Station-month median | 108,607 filled |
| Fill `rh_percent` | Station-month median | 200,877 filled |
| Fill `ws_m_s` | Station-month median | 583,563 filled |
| Fill `wd_deg` | Station-month median | 298,757 filled |
| Fill `rf_mm` | Station-month median | 144,304 filled |
| Fill `sr_w_mt2` | Station-month median | 396,671 filled |
| Fill `bp_mmhg` | Station-month median | 127,774 filled |
| **Total imputed** | | **1,860,553 values** |

**Why station-month median?** Respects seasonal variation — January wind speeds
in Delhi are different from July. An overall mean would introduce inaccurate values.

### Before vs after cleaning

| Metric | Before | After | Change |
|---|---|---|---|
| Rows | 5,831,431 | 5,831,431 | 0 removed |
| Columns | 22 | 19 | 3 dropped |
| Missing cells | 19,402,433 | 11,710,449 | 7,691,984 fixed |
| Data quality | 84.9% | **89.4%** | **+4.6 percentage points** |

### Data transformation

**1. Batch transformation**

| Metric | Value |
|---|---|
| Input rows | 5,831,431 |
| Batches | 24 (one per month) |
| Output rows | 312 aggregated |
| Time | 2.28 seconds |
| Smallest batch | Feb 2024 — 221,832 rows |
| Largest batch | Dec 2024 — 256,766 rows |

**2. MapReduce pattern (PM2.5 classification)**

| Phase | Operation | Result |
|---|---|---|
| MAP | Each row → `(station\|level, 1)` | 628,001 pairs |
| SHUFFLE | Group by key | 50 unique keys |
| REDUCE | Sum counts per key | 50 final counts |

Example (site_103 PM2.5 breakdown):

| Pollution level | Count |
|---|---|
| Very unhealthy | 23,543 |
| Moderate | 13,938 |
| Hazardous | 12,313 |
| Unhealthy | 10,703 |
| Safe | 631 |

**3. ETL pipeline**

| Phase | Output | Rows | Time |
|---|---|---|---|
| EXTRACT | Clean dataset loaded | 5,831,431 | — |
| TRANSFORM → monthly_avg | Avg per pollutant per month | 312 | — |
| TRANSFORM → pm25_aqi | PM2.5 + AQI category label | 628,001 | — |
| TRANSFORM → daily_peaks | Max reading per station per day | 66,101 | — |
| LOAD | Saved to `transformed/` folder | 3 files | 4.13s total |

### 3-way database benchmark (on clean data)

| Method | Query time | Storage type |
|---|---|---|
| Pandas (no DB) | 780.6 ms | In-memory, no SQL |
| SQLite | 1,424.3 ms | Row-based |
| **DuckDB** | **163.0 ms** | Columnar |

DuckDB is **8.7× faster than SQLite** and **4.8× faster than pandas** on the same query.

---

## Week 4 — Data Serving

Cleaned and transformed data loaded into `air_quality_served.duckdb` — the final
output of the pipeline, ready for queries by downstream consumers.

### Tables served

| Table | Rows | Source | Load time |
|---|---|---|---|
| `measurements_clean` | 5,831,431 | Cleaned parquet | 8.86 seconds |
| `stations` | 10 | Distinct from clean data | < 1s |
| `pollutants` | 13 | Distinct from clean data | < 1s |
| `monthly_aggregates` | 312 | ETL output | < 1s |
| `daily_peaks` | 66,101 | ETL output | < 1s |

---

## Full pipeline summary

```
team_4.parquet
(raw, 51.8 MB, 5,831,431 rows × 22 cols)
        │
        ▼ Week 1 — Ingestion & Partitioning
partitioned_data/
(24 Hive partitions by year/month)
        │
        ▼ Week 2 — Data Model
air_quality.duckdb
(star schema, 3 tables, 80.5 MB, loads in 11.7s)
        │
        ▼ Week 3 — Cleaning & Transformation
team_4_clean.parquet
(19 cols, 89.4% quality, 1.86M values imputed)
        +
transformed/
(monthly_avg 312 rows, pm25_aqi 628K rows, daily_peaks 66K rows)
        │
        ▼ Week 4 — Data Serving
air_quality_served.duckdb
(5 tables, clean data, ready to query)
```

---

## All key numbers

| Metric | Value |
|---|---|
| Raw dataset | 5,831,431 rows × 22 columns |
| Parquet file size | 51.8 MB |
| Partitions created | 24 (12 months × 2 years) |
| DuckDB raw database size | 80.5 MB |
| DuckDB load time (5.8M rows) | 11.70 seconds |
| Fastest DuckDB query | 37.8 ms |
| DuckDB vs SQLite speed | **21–43× faster** |
| DuckDB vs SQLite file size | **13.4× smaller** |
| DuckDB vs pandas speed | **4.8× faster** |
| Data quality before cleaning | 84.9% |
| Data quality after cleaning | **89.4%** |
| Columns dropped | 3 |
| Values imputed | 1,860,553 |
| Batch transformation | 5,831,431 rows → 312 aggregated |
| MapReduce keys | 50 unique station-level pairs |
| ETL time | 4.13 seconds |
| Served DB tables | 5 |
| Served DB rows (fact table) | 5,831,431 |
| Served DB load time | 8.86 seconds |

---

## How to reproduce

```bash
pip install pandas pyarrow duckdb missingno matplotlib
```

1. Place `team_4.parquet` in the project root (not in repo — too large)
2. Run `week1_ingestion.ipynb` — produces partitioned data and ingestion summary
3. Run `air_quality_duckdb.ipynb` — builds DuckDB star schema and benchmarks
4. Run `week3_cleaning_eda.ipynb` — cleans, transforms, and serves to database
