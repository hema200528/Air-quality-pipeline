import duckdb
import pandas as pd
import time
from pathlib import Path
from typing import Union
import logging

logger = logging.getLogger(__name__)

def connect_db(db_path: Union[str, Path]) -> duckdb.DuckDBPyConnection:
    """Connects to the DuckDB database file.

    Args:
        db_path: The file path to the DuckDB database, either as a string or Path.

    Returns:
        duckdb.DuckDBPyConnection: The active DuckDB connection object.
    """
    con = duckdb.connect(str(db_path))
    logger.info(f"Connected to DuckDB database: {db_path}")
    return con

def create_raw_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Creates the raw database schema and tables (stations, pollutants, measurements).

    Drops tables if they already exist before creating them.

    Args:
        con: The active DuckDB connection object.
    """
    con.sql("DROP TABLE IF EXISTS measurements")
    con.sql("DROP TABLE IF EXISTS stations")
    con.sql("DROP TABLE IF EXISTS pollutants")
    
    con.sql("""
    CREATE TABLE stations (
        station_id   VARCHAR PRIMARY KEY,
        city         VARCHAR,
        state        VARCHAR
    )
    """)
    
    con.sql("""
    CREATE TABLE pollutants (
        pollutant_id    INTEGER PRIMARY KEY,
        pollutant_name  VARCHAR UNIQUE
    )
    """)
    
    con.sql("""
    CREATE TABLE measurements (
        measurement_id  BIGINT,
        datetime        TIMESTAMP,
        station_id      VARCHAR,
        pollutant_id    INTEGER,
        value           DOUBLE,
        year            INTEGER,
        month           INTEGER,
        day             INTEGER,
        hour            INTEGER,
        at_c            DOUBLE,
        rh_percent      DOUBLE,
        bp_mmhg         DOUBLE,
        ws_m_s          DOUBLE,
        wd_deg          DOUBLE,
        rf_mm           DOUBLE,
        sr_w_mt2        DOUBLE,
        vws_m_s         DOUBLE
    )
    """)
    logger.info("Created raw database tables (stations, pollutants, measurements)")

def load_raw_data(con: duckdb.DuckDBPyConnection, parquet_file: Union[str, Path]) -> None:
    """Populates raw tables from the raw Parquet data.

    Args:
        con: The active DuckDB connection object.
        parquet_file: The path to the source parquet data file.

    Raises:
        FileNotFoundError: If the parquet_file path does not exist.
    """
    if not Path(parquet_file).exists():
        raise FileNotFoundError(f"Parquet file {parquet_file} not found for database load.")
        
    # Load stations
    con.sql(f"""
    INSERT INTO stations
    SELECT DISTINCT station_id, city, state
    FROM '{parquet_file}'
    """)
    
    # Load pollutants
    con.sql(f"""
    INSERT INTO pollutants
    SELECT
        ROW_NUMBER() OVER (ORDER BY pollutant) AS pollutant_id,
        pollutant AS pollutant_name
    FROM (SELECT DISTINCT pollutant FROM '{parquet_file}')
    """)
    
    # Load measurements
    start = time.time()
    con.sql(f"""
    INSERT INTO measurements
    SELECT
        ROW_NUMBER() OVER () AS measurement_id,
        m.datetime,
        m.station_id,
        p.pollutant_id,
        m.value,
        m.year, m.month, m.day, m.hour,
        m.at_c, m.rh_percent, m.bp_mmhg,
        m.ws_m_s, m.wd_deg, m.rf_mm,
        m.sr_w_mt2, m.vws_m_s
    FROM '{parquet_file}' AS m
    JOIN pollutants AS p ON m.pollutant = p.pollutant_name
    """)
    elapsed = time.time() - start
    logger.info(f"Loaded measurements table in {elapsed:.2f} seconds")

def run_benchmark_queries(con: duckdb.DuckDBPyConnection) -> None:
    """Runs a series of performance benchmark queries on the raw database.

    Args:
        con: The active DuckDB connection object.
    """
    def timed_query(label: str, sql: str) -> pd.DataFrame:
        t0 = time.time()
        res = con.sql(sql).df()
        elapsed_ms = (time.time() - t0) * 1000
        logger.info(f"[TIME] {label}: {elapsed_ms:.1f} ms ({len(res):,} rows)")
        return res

    logger.info("Running queries on raw database...")
    
    timed_query("Avg PM2.5 per month", """
        SELECT year, month, AVG(value) AS avg_pm25
        FROM measurements m
        JOIN pollutants p ON m.pollutant_id = p.pollutant_id
        WHERE p.pollutant_name = 'pm25'
        GROUP BY year, month
        ORDER BY year, month
    """)
    
    timed_query("Top 3 stations by avg PM10", """
        SELECT s.station_id, s.city, AVG(m.value) AS avg_pm10
        FROM measurements m
        JOIN pollutants p ON m.pollutant_id = p.pollutant_id
        JOIN stations s   ON m.station_id   = s.station_id
        WHERE p.pollutant_name = 'pm10'
        GROUP BY s.station_id, s.city
        ORDER BY avg_pm10 DESC
        LIMIT 3
    """)
    
    timed_query("Measurement count per pollutant", """
        SELECT p.pollutant_name, COUNT(*) AS n
        FROM measurements m
        JOIN pollutants p ON m.pollutant_id = p.pollutant_id
        GROUP BY p.pollutant_name
        ORDER BY n DESC
    """)

def setup_served_database(
    served_db_path: Union[str, Path],
    df_clean: pd.DataFrame,
    monthly_avg_path: Union[str, Path],
    daily_peaks_path: Union[str, Path]
) -> None:
    """Sets up the final served database containing clean measurements and pre-aggregates.

    Args:
        served_db_path: Path where the served DuckDB file will be created.
        df_clean: The cleaned DataFrame containing measurements.
        monthly_avg_path: Path to the pre-aggregated monthly average parquet file.
        daily_peaks_path: Path to the pre-aggregated daily peaks parquet file.
    """
    con = duckdb.connect(str(served_db_path))
    
    con.sql("DROP TABLE IF EXISTS measurements_clean")
    con.sql("DROP TABLE IF EXISTS stations")
    con.sql("DROP TABLE IF EXISTS pollutants")
    con.sql("DROP TABLE IF EXISTS monthly_aggregates")
    con.sql("DROP TABLE IF EXISTS daily_peaks")
    
    # 1. Dimension Tables
    con.sql("""
        CREATE TABLE stations AS
        SELECT DISTINCT station_id, city, state
        FROM df_clean
        ORDER BY station_id
    """)
    
    con.sql("""
        CREATE TABLE pollutants AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY pollutant) AS pollutant_id,
            pollutant AS pollutant_name
        FROM (SELECT DISTINCT pollutant FROM df_clean)
    """)
    
    # 2. Clean Fact Table
    start = time.time()
    con.sql("""
        CREATE TABLE measurements_clean AS
        SELECT
            ROW_NUMBER() OVER () AS measurement_id,
            m.datetime,
            m.station_id,
            p.pollutant_id,
            m.value,
            m.year, m.month, m.day, m.hour,
            m.at_c, m.rh_percent, m.bp_mmhg,
            m.ws_m_s, m.wd_deg, m.rf_mm, m.sr_w_mt2
        FROM df_clean AS m
        JOIN pollutants p ON m.pollutant = p.pollutant_name
    """)
    elapsed = time.time() - start
    
    # 3. Load pre-aggregated tables from files
    if Path(monthly_avg_path).exists():
        monthly = pd.read_parquet(monthly_avg_path)
        con.register("monthly_df", monthly)
        con.sql("CREATE TABLE monthly_aggregates AS SELECT * FROM monthly_df")
        con.unregister("monthly_df")
        
    if Path(daily_peaks_path).exists():
        peaks = pd.read_parquet(daily_peaks_path)
        con.register("peaks_df", peaks)
        con.sql("CREATE TABLE daily_peaks AS SELECT * FROM peaks_df")
        con.unregister("peaks_df")
        
    logger.info(f"Loaded served database measurements_clean in {elapsed:.2f} seconds")
    logger.info(f"  stations           : {con.sql('SELECT COUNT(*) FROM stations').fetchone()[0]:,}")
    logger.info(f"  pollutants         : {con.sql('SELECT COUNT(*) FROM pollutants').fetchone()[0]:,}")
    logger.info(f"  measurements_clean : {con.sql('SELECT COUNT(*) FROM measurements_clean').fetchone()[0]:,}")
    if con.sql("SHOW TABLES").filter("name = 'monthly_aggregates'").fetchone():
        logger.info(f"  monthly_aggregates : {con.sql('SELECT COUNT(*) FROM monthly_aggregates').fetchone()[0]:,}")
    if con.sql("SHOW TABLES").filter("name = 'daily_peaks'").fetchone():
        logger.info(f"  daily_peaks        : {con.sql('SELECT COUNT(*) FROM daily_peaks').fetchone()[0]:,}")
        
    con.close()


