"""
Control plane: pipeline run tracking, watermarking, and metric logging.

This module owns all observability infrastructure:
- Run lifecycle tracking (start, end, status, row counts)
- Watermark retrieval for incremental processing
- Metric persistence for downstream monitoring

All functions are idempotent and safe to call multiple times.
"""

import logging
import uuid
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

from src.pipeline.config import CONFIG

logger = logging.getLogger("pipeline")


def _spark():
    """Returns the active Spark session (provided by Databricks at runtime)."""
    return SparkSession.getActiveSession()


# ===================================
# CONTROL TABLE INITIALIZATION
# ===================================

def init_control_tables():
    """
    Creates required control-plane Delta tables if they don't already exist.

    Tables created:
    - control.pipeline_runs   : run lifecycle (start/end timestamps, row counts, status)
    - control.pipeline_metrics: flexible key/value metric log per run

    Idempotent — safe to call on every pipeline run.
    """
    spark = _spark()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG["tables"]["runs"]} (
            pipeline  STRING,
            run_id    STRING,
            start_ts  TIMESTAMP,
            end_ts    TIMESTAMP,
            rows_in   INT,
            rows_out  INT,
            status    STRING
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG["tables"]["metrics"]} (
            run_id       STRING,
            pipeline     STRING,
            metric_name  STRING,
            metric_value DOUBLE,
            created_ts   TIMESTAMP
        ) USING DELTA
    """)


# ===================================
# WATERMARK
# ===================================

def get_last_success_ts(pipeline_name: str):
    """
    Returns the end timestamp of the most recent successful run for a pipeline.

    Used as a watermark so each run only processes records that arrived
    after the last successful completion.

    Args:
        pipeline_name (str): Name of the pipeline stage to look up.

    Returns:
        Timestamp of last successful run, or 1900-01-01 if none exists.
    """
    spark = _spark()

    return spark.sql(f"""
        SELECT COALESCE(MAX(end_ts), TIMESTAMP('1900-01-01'))
        FROM {CONFIG["tables"]["runs"]}
        WHERE pipeline = '{pipeline_name}'
          AND status = 'SUCCESS'
    """).collect()[0][0]


# ===================================
# METRICS
# ===================================

def log_metrics(run_id: str, metrics: dict):
    """
    Persists pipeline execution metrics to the control table.

    Each key/value pair in metrics becomes a separate row, allowing
    flexible schema-less metric tracking without table changes.

    Args:
        run_id  (str) : Unique identifier for the pipeline execution.
        metrics (dict): metric_name -> metric_value pairs.
    """
    spark = _spark()
    now = datetime.now()

    rows = [
        (run_id, CONFIG["pipeline_name"], k, float(v), now)
        for k, v in metrics.items()
    ]

    schema = StructType([
        StructField("run_id",       StringType(),  False),
        StructField("pipeline",     StringType(),  False),
        StructField("metric_name",  StringType(),  False),
        StructField("metric_value", DoubleType(),  False),
        StructField("created_ts",   TimestampType(), False),
    ])

    df = spark.createDataFrame(rows, schema)
    df.write.mode("append").saveAsTable(CONFIG["tables"]["metrics"])


# ===================================
# RUN TRACKING
# ===================================

def track_run(pipeline_name: str, func, run_id: str):
    """
    Wraps a pipeline stage with run-lifecycle observability.

    Records start time before execution, then updates the run record
    with end time, row counts, and SUCCESS or FAILED status afterward.

    Args:
        pipeline_name (str)  : Label for this stage (e.g. "bronze", "silver").
        func          (callable): Stage function to execute. Must accept run_id.
        run_id        (str)  : Unique execution identifier.

    Returns:
        dict: Output from the executed function (row counts, stats).

    Raises:
        Exception: Re-raises any exception after logging FAILED status.
    """
    spark = _spark()

    spark.sql(f"""
        INSERT INTO {CONFIG["tables"]["runs"]}
        VALUES('{pipeline_name}', '{run_id}', current_timestamp(), null, null, null, 'RUNNING')
    """)

    try:
        result = func(run_id)

        rows_in  = result.get("rows_in",  0) if isinstance(result, dict) else 0
        rows_out = result.get("rows_out", 0) if isinstance(result, dict) else 0

        spark.sql(f"""
            UPDATE {CONFIG["tables"]["runs"]}
            SET end_ts = current_timestamp(),
                status = 'SUCCESS',
                rows_in = {rows_in},
                rows_out = {rows_out}
            WHERE run_id = '{run_id}'
              AND pipeline = '{pipeline_name}'
        """)

        return result

    except Exception as e:
        spark.sql(f"""
            UPDATE {CONFIG["tables"]["runs"]}
            SET end_ts = current_timestamp(),
                status = 'FAILED'
            WHERE run_id = '{run_id}'
              AND pipeline = '{pipeline_name}'
        """)
        raise e