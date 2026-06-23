import sys
import time
from pathlib import Path
import pandas as pd

from src.ingestion import load_data, generate_summary, partition_dataset
from src.cleaning import clean_data, run_batch_transformation, run_map_reduce_classification, run_etl
from src.database import connect_db, create_raw_schema, load_raw_data, run_benchmark_queries, setup_served_database

def main():
    input_file = "team_4.parquet"
    if not Path(input_file).exists():
        print(f"Error: Source file '{input_file}' not found in the project root directory.")
        print("Please place the 'team_4.parquet' file in the root folder before running the pipeline.")
        print("\nNote: You can run automated tests using mock data by executing:")
        print("   python -m unittest discover -s tests -p \"test_*.py\"\n")
        sys.exit(1)
        
    print("=" * 60)
    print("STARTING AIR QUALITY PIPELINE")
    print("=" * 60)
    
    t_start = time.time()
    
    # 1. INGESTION
    print("\n[Step 1/4] Ingesting Data...")
    df_raw = load_data(input_file)
    generate_summary(df_raw, "output/ingestion_summary.txt")
    partition_dataset(df_raw, "partitioned_data")
    
    # 2. RAW DATABASE LOAD & BENCHMARK
    print("\n[Step 2/4] Setting Up Raw Database...")
    con_raw = connect_db("air_quality.duckdb")
    create_raw_schema(con_raw)
    load_raw_data(con_raw, input_file)
    run_benchmark_queries(con_raw)
    con_raw.close()
    
    # 3. CLEANING & ETL
    print("\n[Step 3/4] Cleaning & Transforming Data...")
    df_clean = clean_data(df_raw)
    df_clean.to_parquet("team_4_clean.parquet", index=False)
    print("[OK] Saved clean dataset to team_4_clean.parquet")
    
    # Run batch transform & MapReduce simulation
    batch_df = run_batch_transformation(df_clean)
    print(f"[OK] Batch transformed: {len(batch_df):,} aggregated rows")
    
    mr_df = run_map_reduce_classification(df_clean)
    print(f"[OK] MapReduce completed: {len(mr_df):,} unique keys classification")
    
    run_etl(df_clean, "transformed")
    
    # 4. SERVED DATABASE SETUP
    print("\n[Step 4/4] Setting Up Serving Database...")
    setup_served_database(
        served_db_path="air_quality_served.duckdb",
        df_clean=df_clean,
        monthly_avg_path="transformed/monthly_avg.parquet",
        daily_peaks_path="transformed/daily_peaks.parquet"
    )
    
    total_time = time.time() - t_start
    print("\n" + "=" * 60)
    print(f"AIR QUALITY PIPELINE EXECUTED SUCCESSFULY in {total_time:.2f} seconds")
    print("=" * 60)

if __name__ == "__main__":
    main()
