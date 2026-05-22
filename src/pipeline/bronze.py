"""
Bronze layer: append-only raw ingestion.

Reads source CSV data and writes it to the bronze Delta table with
ingestion metadata attached. No transformations are applied here —
the bronze layer preserves raw source fidelity for replay and debugging.
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, col

from src.pipeline.config import CONFIG

logger = logging.getLogger("pipeline")


def _spark():
    """Returns the active Spark session (provided by Databricks at runtime)."""
    return SparkSession.getActiveSession()


def bronze_load(run_id: str) -> dict:
    """
    Ingests raw source data into the bronze layer (append-only).

    Reads from the configured CSV source, enriches each record with
    ingestion metadata, and appends to the bronze Delta table partitioned
    by ingest date.

    Metadata columns added:
        - ingest_ts   : timestamp of ingestion
        - ingest_date : partition column derived from ingest_ts
        - source_file : origin file path for traceability
        - run_id      : pipeline execution identifier

    Design guarantees:
        - No transformations beyond metadata enrichment
        - Append-only (safe to re-run; downstream layers use watermarks)
        - Full source lineage via source_file column

    Args:
        run_id (str): Unique identifier for the pipeline execution.

    Returns:
        dict: { rows_in: int, rows_out: int }
    """
    spark = _spark()

    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .load(CONFIG["source"])
        .withColumn("ingest_ts",   current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("run_id",      F.lit(run_id))
    )

    row_count = df_raw.count()

    df_raw.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("ingest_date") \
        .saveAsTable(CONFIG["tables"]["bronze"])

    logger.info(f"[run_id={run_id}] Bronze rows ingested: {row_count}")

    return {"rows_in": row_count, "rows_out": row_count}