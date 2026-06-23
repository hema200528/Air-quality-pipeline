import unittest
import pandas as pd
import numpy as np
import shutil
from pathlib import Path
import duckdb

from src.ingestion import generate_summary, partition_dataset
from src.cleaning import clean_data, run_batch_transformation, run_map_reduce_classification, run_etl
from src.database import create_raw_schema, setup_served_database

class TestAirQualityPipeline(unittest.TestCase):
    
    def setUp(self):
        # Create a mock dataset representative of the CPCB Delhi air quality schema
        self.mock_data = pd.DataFrame({
            'station_id': ['site_101', 'site_101', 'site_102', 'site_102', 'site_101'],
            'state': ['Delhi', 'Delhi', 'Delhi', 'Delhi', 'Delhi'],
            'city': ['Delhi', 'Delhi', 'Delhi', 'Delhi', 'Delhi'],
            'station_name': ['Alipur', 'Alipur', 'DTU', 'DTU', 'Alipur'],
            'timestamp': ['2024-01-01T00:00:00Z', '2024-01-01T01:00:00Z', '2024-01-01T00:00:00Z', '2024-01-01T01:00:00Z', '2024-01-01T00:00:00Z'], # last one is duplicate row
            'datetime': pd.to_datetime(['2024-01-01 00:00:00', '2024-01-01 01:00:00', '2024-01-01 00:00:00', '2024-01-01 01:00:00', '2024-01-01 00:00:00']),
            'at_c': [10.5, 12.0, np.nan, 14.5, 10.5], # one nan for median imputation
            'rh_percent': [80.0, 75.0, 85.0, np.nan, 80.0],
            'ws_m_s': [1.2, 1.5, 0.8, 1.0, 1.2],
            'wd_deg': [45.0, 50.0, 120.0, 130.0, 45.0],
            'rf_mm': [0.0, 0.0, 0.0, 0.0, 0.0],
            'tot_rf_mm': [0.0, 0.0, 0.0, 0.0, 0.0],
            'sr_w_mt2': [10.0, 50.0, np.nan, 40.0, 10.0],
            'bp_mmhg': [750.0, 751.0, 749.0, 750.0, 750.0],
            'vws_m_s': [np.nan, np.nan, np.nan, np.nan, np.nan], # 100% null column
            'pollutant': ['pm25', 'pm25', 'pm25', 'pm10', 'pm25'],
            'value': [45.0, -10.0, 12.0, 85.0, 45.0], # one negative value, one duplicate row
            'station': ['Alipur', 'Alipur', 'DTU', 'DTU', 'Alipur'],
            'year': [2024, 2024, 2024, 2024, 2024],
            'month': [1, 1, 1, 1, 1],
            'day': [1, 1, 1, 1, 1],
            'hour': [0, 1, 0, 1, 0]
        })
        self.temp_dir = Path("temp_test_outputs")
        self.temp_dir.mkdir(exist_ok=True)
        
    def tearDown(self):
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            
    def test_generate_summary(self):
        summary_path = self.temp_dir / "summary.txt"
        summary = generate_summary(self.mock_data, str(summary_path))
        self.assertTrue(summary_path.exists())
        self.assertIn("Total records      : 5", summary)
        self.assertIn("Stations           : 2", summary)
        self.assertIn("Pollutant species  : 2", summary)
        
    def test_partition_dataset(self):
        partition_path = self.temp_dir / "partitions"
        partition_dataset(self.mock_data, str(partition_path))
        self.assertTrue((partition_path / "year=2024" / "month=01" / "data.parquet").exists())
        
        # Load partition and check columns (year and month should be dropped)
        partitioned_df = pd.read_parquet(partition_path / "year=2024" / "month=01" / "data.parquet")
        self.assertNotIn("year", partitioned_df.columns)
        self.assertNotIn("month", partitioned_df.columns)
        
    def test_clean_data(self):
        # Cleaning should:
        # - Drop 'vws_m_s', 'timestamp', 'station'
        # - Remove negative value (row 1, pm25=-10.0)
        # - Remove duplicate row (row 4, identical to row 0)
        # - Impute weather columns
        cleaned = clean_data(self.mock_data)
        
        # Check dropped columns
        self.assertNotIn("vws_m_s", cleaned.columns)
        self.assertNotIn("timestamp", cleaned.columns)
        self.assertNotIn("station", cleaned.columns)
        
        # Check rows:
        # Row 1 (negative) and Row 4 (duplicate) are removed, leaving 3 rows
        self.assertEqual(len(cleaned), 3)
        self.assertTrue((cleaned['value'] >= 0).all())
        
        # Check weather imputation for 'at_c' and 'rh_percent'
        # site_102 month 1 median for 'at_c' was np.nan initially because there was only 1 row.
        # site_101 month 1 median for 'rh_percent' was 80.0
        # site_102 month 1 median for 'rh_percent' was 85.0
        # Check that rh_percent on site_102 row 3 got filled with 85.0
        self.assertFalse(cleaned['rh_percent'].isna().any())
        
    def test_batch_transformation(self):
        cleaned = clean_data(self.mock_data)
        transformed = run_batch_transformation(cleaned)
        self.assertEqual(len(transformed), 2) # pm2.5 and pm10
        self.assertIn("avg_value", transformed.columns)
        
    def test_map_reduce_classification(self):
        cleaned = clean_data(self.mock_data)
        mr_results = run_map_reduce_classification(cleaned)
        # site_101 pm25 value = 45.0 (unhealthy)
        # site_102 pm25 value = 12.0 (moderate)
        self.assertEqual(len(mr_results), 2)
        site_101_res = mr_results[mr_results['station'] == 'site_101'].iloc[0]
        self.assertEqual(site_101_res['pollution_level'], 'unhealthy')
        
    def test_duckdb_schema_and_served(self):
        # We can test schema creation and served DB on an in-memory DuckDB
        con = duckdb.connect()
        create_raw_schema(con)
        tables = [t[0] for t in con.sql("SHOW TABLES").fetchall()]
        self.assertIn("measurements", tables)
        self.assertIn("stations", tables)
        self.assertIn("pollutants", tables)
        con.close()
        
        # Test served DB creation
        cleaned = clean_data(self.mock_data)
        etl_out = run_etl(cleaned, str(self.temp_dir))
        
        setup_served_database(
            served_db_path=str(self.temp_dir / "served.duckdb"),
            df_clean=cleaned,
            monthly_avg_path=str(self.temp_dir / "monthly_avg.parquet"),
            daily_peaks_path=str(self.temp_dir / "daily_peaks.parquet")
        )
        
        con_served = duckdb.connect(str(self.temp_dir / "served.duckdb"))
        served_tables = [t[0] for t in con_served.sql("SHOW TABLES").fetchall()]
        self.assertIn("measurements_clean", served_tables)
        self.assertIn("stations", served_tables)
        self.assertIn("pollutants", served_tables)
        self.assertIn("monthly_aggregates", served_tables)
        self.assertIn("daily_peaks", served_tables)
        con_served.close()

if __name__ == "__main__":
    unittest.main()
