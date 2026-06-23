import pandas as pd
from pathlib import Path
import shutil
from typing import Union
import logging

logger = logging.getLogger(__name__)

def load_data(filepath: Union[str, Path]) -> pd.DataFrame:
    """Loads the raw Parquet file into a Pandas DataFrame.

    Args:
        filepath: The path to the parquet file to load, either as a string or Path.

    Returns:
        pd.DataFrame: The loaded DataFrame containing raw air quality data.

    Raises:
        FileNotFoundError: If the provided filepath does not exist.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Source file {filepath} not found.")
    df = pd.read_parquet(path)
    logger.info(f"Loaded {len(df):,} rows x {df.shape[1]} columns")
    return df

def generate_summary(df: pd.DataFrame, output_path: Union[str, Path] = "output/ingestion_summary.txt") -> str:
    """Generates summary statistics from the data and saves them to a file.

    Args:
        df: The DataFrame containing air quality data to summarize.
        output_path: The file path where the summary report will be saved.

    Returns:
        str: The raw content of the generated summary report.
    """
    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    
    summary_lines = [
        "============================================================",
        "INGESTION SUMMARY",
        "============================================================",
        f"Total records      : {len(df):,}",
        f"Columns            : {df.shape[1]}",
        f"Date range         : {df['datetime'].min()}  ->  {df['datetime'].max()}",
        f"Years              : {sorted(df['year'].unique().tolist())}",
        f"Stations           : {df['station_id'].nunique()}",
        f"Pollutant species  : {df['pollutant'].nunique()}",
        "",
        "Records per year-month:",
        df.groupby(["year", "month"]).size().to_string(),
        "",
        "Records per pollutant:",
        df["pollutant"].value_counts().to_string(),
    ]
    summary_content = "\n".join(summary_lines)
    out_file.write_text(summary_content, encoding="utf-8")
    logger.info(f"Saved ingestion summary to {output_path}")
    return summary_content

def partition_dataset(df: pd.DataFrame, output_dir: Union[str, Path] = "partitioned_data") -> int:
    """Partitions the dataset in Hive-style format (year=YYYY/month=MM/data.parquet).

    Args:
        df: The DataFrame containing air quality data to partition.
        output_dir: The root directory where partitions will be saved.

    Returns:
        int: The total number of partition folders/files written.
    """
    out_path = Path(output_dir)
    
    # Clean previous partitioning
    if out_path.exists():
        shutil.rmtree(out_path)
    out_path.mkdir(parents=True, exist_ok=True)
    
    written = 0
    for (yr, mo), chunk in df.groupby(["year", "month"]):
        folder = out_path / f"year={yr}" / f"month={mo:02d}"
        folder.mkdir(parents=True, exist_ok=True)
        # Drop partitioning columns from file content to avoid redundancy
        chunk.drop(columns=["year", "month"]).to_parquet(folder / "data.parquet", index=False)
        written += 1
        logger.info(f"  Partition year={yr} month={mo:02d}  ->  {len(chunk):,} rows")
        
    logger.info(f"Wrote {written} partition files into '{output_dir}/'")
    return written


