import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict
from functools import reduce as ft_reduce

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Performs full data cleaning on the raw DataFrame."""
    df_clean = df.copy()
    
    # 1. Drop 100% null column
    if 'vws_m_s' in df_clean.columns:
        df_clean = df_clean.drop(columns=['vws_m_s'])
        print("[OK] Dropped 'vws_m_s' (100% null)")
        
    # 2. Drop duplicate timestamp column (VARCHAR version)
    if 'timestamp' in df_clean.columns:
        df_clean = df_clean.drop(columns=['timestamp'])
        print("[OK] Dropped 'timestamp' (duplicate of datetime)")
        
    # 3. Drop duplicate station column
    if 'station' in df_clean.columns:
        df_clean = df_clean.drop(columns=['station'])
        print("[OK] Dropped 'station' (duplicate of station_id)")
        
    # 4. Remove negative pollution values (physically impossible)
    if 'value' in df_clean.columns:
        neg_mask = df_clean['value'] < 0
        neg_count = neg_mask.sum()
        df_clean = df_clean[~neg_mask]
        print(f"[OK] Removed {neg_count:,} negative pollution readings")
        
    # 5. Remove duplicate rows
    dupes_count = df_clean.duplicated().sum()
    df_clean = df_clean.drop_duplicates()
    print(f"[OK] Removed {dupes_count:,} duplicate rows")
    
    # 6. Fill missing weather values with station-month median
    weather_cols = ['at_c', 'rh_percent', 'ws_m_s', 'wd_deg', 'rf_mm', 'tot_rf_mm', 'sr_w_mt2', 'bp_mmhg']
    for col in weather_cols:
        if col in df_clean.columns:
            before = df_clean[col].isna().sum()
            df_clean[col] = df_clean.groupby(['station_id', 'month'])[col].transform(
                lambda x: x.fillna(x.median())
            )
            after = df_clean[col].isna().sum()
            print(f"[OK] Filled {before - after:,} missing values in '{col}' with station-month median")
            
    return df_clean

def run_batch_transformation(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregates pollution readings per month and pollutant."""
    results = []
    for (year, month), batch in df.groupby(['year', 'month']):
        batch_stats = batch.groupby('pollutant')['value'].agg(
            ['mean', 'max', 'min', 'count', 'std']
        ).round(2)
        batch_stats['year'] = year
        batch_stats['month'] = month
        results.append(batch_stats)
        
    batch_transformed = pd.concat(results).reset_index()
    batch_transformed.rename(columns={
        'mean': 'avg_value', 'max': 'max_value',
        'min': 'min_value', 'count': 'reading_count', 'std': 'std_value'
    }, inplace=True)
    return batch_transformed

def run_map_reduce_classification(df: pd.DataFrame) -> pd.DataFrame:
    """Simulates MapReduce to classify PM2.5 levels by station."""
    pm25_df = df[df['pollutant'] == 'pm25'].copy()
    
    # MAP
    def map_fn(row):
        val = row['value']
        if val <= 5:        level = 'safe'
        elif val <= 35:    level = 'moderate'
        elif val <= 55:    level = 'unhealthy'
        elif val <= 150:   level = 'very_unhealthy'
        else:              level = 'hazardous'
        return (f"{row['station_id']}|{level}", 1)
        
    mapped = pm25_df.apply(map_fn, axis=1).tolist()
    
    # SHUFFLE
    shuffled = defaultdict(list)
    for key, val in mapped:
        shuffled[key].append(val)
        
    # REDUCE
    reduced = {k: ft_reduce(lambda a, b: a + b, v) for k, v in shuffled.items()}
    
    # FORMAT
    mr_result = pd.DataFrame([
        {'station': k.split('|')[0],
         'pollution_level': k.split('|')[1],
         'count': v}
        for k, v in reduced.items()
    ]).sort_values(['station', 'count'], ascending=[True, False])
    
    return mr_result

def run_etl(df_clean: pd.DataFrame, output_dir: str = "transformed") -> dict:
    """Runs the full ETL process and saves the files."""
    print("=== ETL PIPELINE ===")
    
    # Extract
    print(f"  [EXTRACT] {len(df_clean):,} rows from clean dataset")
    
    # Transform
    out = {}
    
    # Monthly averages per pollutant
    out['monthly_avg'] = df_clean.groupby(
        ['year', 'month', 'pollutant']
    )['value'].mean().round(2).reset_index()
    
    # PM2.5 with AQI category
    pm25 = df_clean[df_clean['pollutant'] == 'pm25'].copy()
    pm25['aqi_category'] = pd.cut(
        pm25['value'],
        bins=[0, 5, 35, 55, 150, float('inf')],
        labels=['Safe', 'Moderate', 'Unhealthy', 'Very Unhealthy', 'Hazardous']
    )
    out['pm25_aqi'] = pm25[['datetime', 'station_id', 'value', 'aqi_category']]
    
    # Daily peak per station
    out['daily_peaks'] = df_clean.groupby(
        ['year', 'month', 'day', 'station_id', 'pollutant']
    )['value'].max().reset_index()
    
    print(f"  [TRANSFORM] 3 tables produced:")
    for name, table in out.items():
         print(f"    {name}: {len(table):,} rows")
         
    # Load
    out_path = Path(output_dir)
    out_path.mkdir(exist_ok=True)
    for name, table in out.items():
        path = out_path / f"{name}.parquet"
        table.to_parquet(path, index=False)
        print(f"  [LOAD] Saved {path}")
        
    return out
