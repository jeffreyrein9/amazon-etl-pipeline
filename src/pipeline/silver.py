"""
Silver layer: cleaning, validation, deduplication, and idempotent merge.

Reads incrementally from bronze using watermark logic, applies structured
cleaning and validation rules, deduplicates via row hashing, and merges
results into the silver Delta table.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, col, lower, trim
from delta.tables import DeltaTable  # type: ignore

from src.pipeline.config import CONFIG
from src.pipeline.control import get_last_success_ts

logger = logging.getLogger("pipeline")


def _spark():
    """Returns the active Spark session (provided by Databricks at runtime)."""
    return SparkSession.getActiveSession()


# ===================================
# CLEAN
# ===================================

def clean(df: DataFrame) -> DataFrame:
    """
    Applies data quality transformations to raw bronze data.

    Steps:
        1. Type casting   : safe-casts all columns to configured types
        2. Null handling  : fills missing values with configured defaults
        3. Standardization: trims whitespace, lowercases categorical fields

    Does not filter or remove records — pure transformation only.

    Args:
        df (DataFrame): Raw bronze-level DataFrame.

    Returns:
        DataFrame: Cleaned and standardized DataFrame ready for validation.
    """
    # 1) casts
    df_cast = df
    for c, dtype in CONFIG["schema"]["casts"].items():
        df_cast = df_cast.withColumn(c, F.expr(f"try_cast({c} AS {dtype})"))

    # 2) handle nulls
    df_clean = df_cast.fillna(CONFIG["schema"]["fill_defaults"]["numeric"])
    df_clean = df_clean.fillna(CONFIG["schema"]["fill_defaults"]["categorical"])
    df_clean = df_clean.fillna(CONFIG["schema"]["fill_defaults"]["boolean"])

    # 3) standardize
    df_std = df_clean
    for c in CONFIG["schema"]["standardize_cols"]:
        df_std = df_std.withColumn(c, lower(trim(col(c))))

    return df_std


# ===================================
# VALIDATE
# ===================================

def validate(df_std: DataFrame, run_id: str):
    """
    Validates cleaned data for completeness and separates rejects.

    Checks that all required columns are non-null and non-empty.
    Rejected rows are written to the rejects control table with
    the full row payload and reject reason for debugging.

    Args:
        df_std  (DataFrame): Standardized input DataFrame after cleaning.
        run_id  (str)      : Pipeline execution identifier.

    Returns:
        tuple:
            - df_valid     (DataFrame): Rows passing all validation checks.
            - reject_count (int)      : Number of rejected records.
    """
    spark = _spark()

    reject_cond = None
    for c in CONFIG["data_quality"]["required_cols"]:
        cond = col(c).isNull() | (trim(col(c)) == "")
        reject_cond = cond if reject_cond is None else reject_cond | cond

    df_reject = df_std.filter(reject_cond)
    df_valid  = df_std.filter(~reject_cond)
    reject_count = df_reject.count()

    if reject_count > 0:
        (
            df_reject
            .withColumn("payload",       F.to_json(F.struct("*")))
            .withColumn("reject_reason", F.lit("missing_required_column"))
            .withColumn("run_id",        F.lit(run_id))
            .withColumn("created_ts",    current_timestamp())
            .select("run_id", "reject_reason", "payload", "created_ts")
            .write.mode("append")
            .saveAsTable(CONFIG["tables"]["rejects"])
        )

    return df_valid, reject_count


# ===================================
# SILVER TRANSFORM
# ===================================

def silver_transform(run_id: str) -> dict:
    """
    Executes incremental ETL from bronze to silver layer.

    Pipeline stages:
        1. Incremental read : filters bronze records newer than last successful run
        2. Clean            : type casting, null filling, standardization
        3. Validate         : required field checks, reject logging
        4. Hash             : deterministic row hash for idempotency
        5. Deduplicate      : drops duplicate rows by hash
        6. Merge            : idempotent upsert into silver Delta table

    Args:
        run_id (str): Unique execution identifier.

    Returns:
        dict: {
            rows_in      : number of bronze records processed,
            rows_out     : deduplicated records written to silver,
            rows_rejected: records failing validation,
        }
    """
    spark = _spark()

    silver_last_ts = get_last_success_ts(CONFIG["pipelines"]["silver"])

    # 1) incremental read
    df = (
        spark.table(CONFIG["tables"]["bronze"])
        .filter(col(CONFIG["watermark_column"]) > F.lit(silver_last_ts))
    )

    if df.limit(1).count() == 0:
        logger.info("Silver: no new data to process.")
        return {"rows_in": 0, "rows_out": 0, "rows_rejected": 0}

    rows_in = df.count()

    # 2) clean
    df_clean = clean(df)

    # 3) validate
    df_valid, reject_count = validate(df_clean, run_id)

    # 4) hash (idempotency key)
    df_hash = df_valid.withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws("||", *[col(c) for c in CONFIG["data_quality"]["dedup_keys"]]),
            256
        )
    )

    # 5) deduplicate
    df_final = df_hash.dropDuplicates(["row_hash"])
    rows_out = df_final.count()

    # 6) ensure table exists, then merge
    if not spark.catalog.tableExists(CONFIG["tables"]["silver"]):
        df_final.write.format("delta").saveAsTable(CONFIG["tables"]["silver"])

    target = DeltaTable.forName(spark, CONFIG["tables"]["silver"])

    target.alias("t").merge(
        df_final.alias("s"),
        "t.row_hash = s.row_hash"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    logger.info(
        f"[run_id={run_id}] Silver — in: {rows_in}, out: {rows_out}, rejected: {reject_count}"
    )

    return {
        "rows_in":       rows_in,
        "rows_out":      rows_out,
        "rows_rejected": reject_count,
    }