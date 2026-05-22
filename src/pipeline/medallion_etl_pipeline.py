"""
Incremental batch pipeline (Bronze -> Silver -> Gold)
Author: Jeffrey Rein

Description:
This pipeline implements an incremental ETL workflow for Amazon e-commerce data, following a
Medallion Architecture (Bronze, Silver, Gold layers) using Apache Spark and Delta Lake.

This pipeline supports:
- Append-only raw ingestion (Bronze layer)
- Data cleaning, validation, and deduplication (Silver layer)
- Incremental aggregate computation (Gold layer)
- Run-level observability via control tables (metrics, lineage, and status tracking)

Data Source:
Amazon e-commerce dataset (Kaggle)
https://www.kaggle.com/datasets/sharmajicoder/amazon-e-commerce

Configuration Notes (for adaptation to new datasets):
When repurposing this pipeline, update:
1) CONFIG (source path, schemas, and pipeline name)
2) Gold-layer business aggregations
3) Table names and database environments
4) Watermark column (if ingestion logic changes)
5) Deduplication keys and required fields

Design Goals:
- Idempotent execution
- Incremental processing via watermarking
- Re-runnable without duplicate side effects
- Clear separation of transformation layers
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name, current_timestamp, col, lower, trim
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from delta.tables import DeltaTable # type:ignore
import logging
import uuid
from datetime import datetime

# ===================================
# INIT
# ===================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pipeline")

"""spark = SparkSession.builder \
    .appName("AmazonPipeline") \
    .getOrCreate()"""

# ===================================
# CONFIG
# ===================================
CONFIG = {
    "pipeline_name": "amazon_etl",

    "pipelines": {
        "silver": "amazon_silver",
        "gold": "amazon_gold"
    },

    "source": "/Volumes/workspace/default/amazon_ecommerce/amazon_ecommerce_1M.csv",
    "paths": {
#        "bronze": "dbfs:/tmp/amazon_ecommerce/bronze",
#        "silver": "dbfs:/tmp/amazon_ecommerce/silver",
#        "gold": "dbfs:/tmp/amazon_ecommerce/gold",
        "bronze": "/Volumes/workspace/default/amazon_ecommerce/bronze",
        "silver": "/Volumes/workspace/default/amazon_ecommerce/silver",
        "gold": "/Volumes/workspace/default/amazon_ecommerce/gold",
    },

    "tables": {
        "bronze": "bronze.amazon_raw",
        "silver": "silver.amazon_clean",

        "daily_revenue": "gold.daily_revenue",
        "user_metrics": "gold.user_metrics",
        "product_metrics": "gold.product_metrics",
        "shipping_metrics": "gold.shipping_metrics",
        "seller_metrics": "gold.seller_metrics",
        "category_metrics": "gold.category_metrics",
        "delivery_metrics": "gold.delivery_metrics",

        "runs": "control.pipeline_runs",
        "metrics": "control.pipeline_metrics",
        "rejects": "control.pipeline_rejects",
    },

    "watermark_column": "ingest_ts",

    "data_quality": {
        "required_cols": [
            "user_id",
            "product_id",
            "seller_id",
            "purchase_date",
            "price",
            "final_price"
        ],

        "dedup_keys": [
            "user_id",
            "product_id",
            "purchase_date",
            "seller_id"
        ]
    },

    "schema": {
        "casts": {
            "user_id": "string",
            "product_id": "string",
            "category": "string",
            "subcategory": "string",
            "brand": "string",
            "price": "double",
            "discount": "double",
            "final_price": "double",
            "rating": "double",
            "review_count": "integer",
            "stock": "integer",
            "seller_id": "string",
            "seller_rating": "double",
            "purchase_date": "date",
            "shipping_time_days": "integer",
            "location": "string",
            "device": "string",
            "payment_method": "string",
            "is_returned": "boolean",
            "delivery_status": "string"
        },

        "numeric_cols": [
            "price",
            "final_price",
            "discount",
            "rating",
            "review_count"
        ],

        "fill_defaults": {
            "numeric": {
                "discount": 0.0,
                "review_count": 0
            },
            "categorical": {
                "category": "unknown",
                "subcategory": "unknown",
                "brand": "unknown",
                "location": "unknown",
                "device": "unknown",
                "payment_method": "unknown",
                "delivery_status": "unknown"
            },
            "boolean": {
                "is_returned": False
            }
        },

        "standardize_cols": [
            "category",
            "subcategory",
            "brand",
            "location",
            "device",
            "payment_method",
            "delivery_status",
        ],
    },
}

# ===================================
# CONTROL TABLES
# ===================================
def init_control_tables():
    """
    Initializes required contol-plane tables for pipeline tracking
    
    Creates Delta tables for:
    - pipeline run tracking (start/end timestamps, status, row counts)
    - metric logging (custom KPIs per run)
    
    This function is idempotent and safe to run multiple times
    It ensures observability infrastructure exists before any ETL execution
    """

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CONFIG["tables"]["runs"]} (
        pipeline STRING,
        run_id STRING,
        start_ts TIMESTAMP,
        end_ts TIMESTAMP,
        rows_in INT,
        rows_out INT,
        status STRING
    )
    USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONFIG["tables"]["metrics"]} (
            run_id STRING,
            pipeline STRING,
            metric_name STRING,
            metric_value DOUBLE,
            created_ts TIMESTAMP
        ) USING DELTA
        """)

# ===================================
# WATERMARK (RUN-BASED)
# ===================================
def get_last_success_ts(pipeline_name: str):
    """
    Retrieves the most recent successful pipeline completion timestamp
    
    Used as a watermark to enable incremental processing by filtering only data that arrived after the last successful run
    
    Args:
        pipeline_name (str): Name of the pipeline to retrieve watermark for
    Returns:
        timestamp of last successful run end time, or a default epoch timestamp if no successful runs exist
    """

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
    Persists pipeline execution metrics into the control table
    
    Each metric is stored as a separate row associated with a run_id, allowing flexible schema-less metric tracking
    
    Args:
        run_id (str): Unique identifier for the pipeline execution
        metrics (dict): Dictionary of metric_name -> metric_value pairs
    Behaviors:
        Converts metrics into a structured DataFrame
        Adds metadata (pipeline name, timestamp)
        Appends results to the metrics Delta table
        """
    
    now = datetime.now()
    rows = [
        (run_id, CONFIG["pipeline_name"], k, float(v), now)
        for k, v in metrics.items()
    ]

    schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("metric_value", DoubleType(), False),
        StructField("created_ts", TimestampType(), False)
    ])

    df = spark.createDataFrame(rows, schema)
    df.write.mode("append").saveAsTable(CONFIG["tables"]["metrics"])

# ===================================
# BRONZE - raw ingestion (append-only)
# ===================================
def bronze_load(run_id: str):
    """
    Ingests raw source data into the bronze layer in an append-only format

    This function represents the entry point of the ETL pipeline. It reads raw data from the configured source location,
    enriches it with ingestion metadata, and persists it into a partitioned Delta table for downstream processing

    Processing behavior:
    - Reads raw input data from the configured source path
    - Applies no transformations beyond metadata enrichment
    - Adds ingestion metadata columns:
        - ingest_ts: timestamp of ingestion
        - ingest_date: derived partition column for storate optimization
        - source_file: origin file path for traceability
        - run_id: pipeline execution identifier
    - Writes data in append-only mode to the bronze Delta location
    - Ensures the bronze table exists before writing

    Design guarantees:
    - Idempotent with respect to downstream processing (append-only storage)
    - Preserves raw source fidelity without modification
    - Enables replay/debugging via full source lineage tracking

    Args:
        run_id(str): Unique identifier for the pipeline execution
    Returns:
        dict: Dictionary with rows_in and rows_out counts
    """

    # 1) read files from source, return DataFrame with timestamps/source
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .load(CONFIG["source"])
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("ingest_date", F.to_date("ingest_ts"))
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("run_id", F.lit(run_id))
    )

    row_count = df_raw.count()

    # 2) append + save to bronze data lake
    df_raw.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("ingest_date") \
        .saveAsTable(CONFIG["tables"]["bronze"])

    logger.info(f"[run_id={run_id}] Bronze rows: {row_count}")
    return {"rows_in": row_count, "rows_out": row_count}

# ===================================
# SILVER - clean, standardize, dedupe, idempotent
# ===================================
def clean(df: DataFrame) -> DataFrame:
    """
    Applies data quality transformations to prepare raw data for analytical processing

    This function performs structured cleansing operations to standardize schema, enforce data types,
    and normalize categorical values prior to validation and downstream transformations in the silver layer

    Processing steps:
    1) Type casting:
        - Converts columns to their configured data types using safe casting logic
        - Prevents pipeline failures from invalid type conversions
    2) Null handling:
        - Fills missing numeric values with configured defaults
        - Fills missing categorical values with standardized placeholders
        - Fills missing boolean values with default boolean states
    3) Standardization:
        - Trims whitespace and converts categorical text fields to lowercase
        - Ensures consistency for grouping, joins, and deduplication logic

    Design guarantees:
    - Produces schema-consistent output suitable for validation and deduplication
    - Ensures deterministic transformations across repeated pipeline runs
    - Does not remove or filter records (pure transformation step)

    Args:
        df (DataFrame): Raw or bronze-level DataFrame requiring standardization
    Returns:
        DataFrame: Cleaned and standardized DataFrame ready for validation
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

# 4) reject rows if missing required columns
def validate(df_std: DataFrame, run_id: str):
    """
    Validates cleaned silver-layer data for completeness and integrity
    
    Checks:
    - required columns are not null or empty
    - generates reject records for invalid rows
    
    Args:
        df_std (DataFrame): Standardized input DataFrame after cleaning
        run_id (str): Pipeline execution identifier
    Returns:
        tuple:
        - df_valid (DataFrame): Rows passing validation checks
        - reject_count (int): Number of rejected records
    Side Effects:
        - writes rejected rows to the rejects control table
        stores full row payload for debugging
    """

    reject_cond = None
    for c in CONFIG["data_quality"]["required_cols"]:
        cond = col(c).isNull() | (trim(col(c)) == "")
        reject_cond = cond if reject_cond is None else reject_cond | cond

    df_reject = df_std.filter(reject_cond)
    df_valid = df_std.filter(~reject_cond)
    reject_count = df_reject.count()

    if reject_count > 0:
        (
            df_reject
            .withColumn("payload", F.to_json(F.struct("*")))
            .withColumn("reject_reason", F.lit("missing_required_column"))
            .withColumn("run_id", F.lit(run_id))
            .withColumn("created_ts", current_timestamp())
            .select("run_id", "reject_reason", "payload", "created_ts")
            .write.mode("append")
            .saveAsTable(CONFIG["tables"]["rejects"])
        )

    return df_valid, reject_count

def silver_transform(run_id: str):
    """
    Executes incremental ETL from bronze -> silver layer

    Pipeline stages:
    1) Reads only new records using watermark logic
    2) Applies cleaning and standardization rules
    3) Validates required fields
    4) Deduplicates using deterministic row hashing
    5) Performs idempotent merge into silver Delta table

    Args:
        run_id (str): Unique execution identifier
    Returns:
        dict:
            - rows_in: number of input records processed
            - rows_out: number of deduplicated output records
            - rows_rejected: invalid records removed during validation
            - duplicates: detected duplicate records
    """

    silver_last_ts = get_last_success_ts(CONFIG["pipelines"]["silver"])

    # 1) incremental read
    df = spark.table(CONFIG["tables"]["bronze"]) \
        .filter(col(CONFIG["watermark_column"]) > F.lit(silver_last_ts))
    
    if df.limit(1).count() == 0:
        logger.info("No new data to process")
        return {
            "rows_in": 0,
            "rows_out": 0,
            "rows_rejected": 0,
            "duplicates": 0
        }

    rows_in = df.count()

    # 2) clean
    df_clean = clean(df)

    # 3) validate
    df_valid, reject_count = validate(df_clean, run_id)

    # 4) hash (idempotency)
    df_hash = df_valid.withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[col(c) for c in CONFIG["data_quality"]["dedup_keys"]]
            ),
            256
        )
    )

    # 5) dedup
    df_final = df_hash.dropDuplicates(["row_hash"])
    rows_out = df_final.count()

    # 6) ensure table exists
    if not spark.catalog.tableExists(CONFIG["tables"]["silver"]):
        (
            df_final
            .write
            .format("delta")
            .saveAsTable(CONFIG["tables"]["silver"])
        )

    target = DeltaTable.forName(spark, CONFIG["tables"]["silver"])

    # 7) idempotent merge
    target.alias("t").merge(
        df_final.alias("s"),
        "t.row_hash = s.row_hash"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    return {
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_rejected": reject_count
    }

# ===================================
# GOLD - business aggregates
# ===================================
def build_gold():
    """
    Orchestrates incremental computation of gold-layer analytical aggregates

    This function serves as the entry point for business-level transformations. It identifies changed
    records in the silver layer and triggers partial recomputation of only affected aggregation groups,
    ensuring efficient updates to downstream analytical talbes

    Processing logic:
    1) Determines incremental changes in the silver dataset using watermark-based filtering
    2) If no new data is detected, exits early without computation
    3) Delegates aggregation logic to 'write_gold_tables', which:
        - Identifies impacted dimensional keys
        - Recomputes full aggregates for only affected groups
        - Applies idempotent merge operations into gold tables

    Incremental strategy:
    - Uses changed dimension keys rather than full dataset recomputation
    - Ensures correctness by fully recalculating impacted groups
    - Minimizes compute cost by limiting scope to modified partitions

    Design guarantees:
    - Idempotent execution across repeated pipeline runs
    - Efficient partial recomputation of analytical aggregates
    - Consistency between silver updates and gold metrics

    Returns:
        None
    """

    gold_last_ts = get_last_success_ts(CONFIG["pipelines"]["gold"])
    df_silver = spark.table(CONFIG["tables"]["silver"])
    df_incremental = df_silver.filter(
        col("ingest_ts") > F.lit(gold_last_ts)
    )

    if df_incremental.limit(1).count() == 0:
        logger.info("No changes for gold")
        return None
    


    write_gold_tables(df_silver, df_incremental)

# overwrite gold tables
def write_gold_tables(df_silver, df_incremental):
    """
    Incrementally updates gold-layer aggregation tables

    Strategy:
    - Detects changed dimension keys from incremental silver data
    - Recomputes full aggregates for only affected groups
    - Performs idempotent merge into gold Delta tables

    This ensures:
    - Efficient recomputation (only impatcted partitions)
    - Correctness (full group recalculation per changed key)
    - Idempotency across repeated runs

    Args:
        df_silver (DataFrame): Full silver dataset
        df_incremental (DataFrame): Newly ingested silver records
    """

    # lambda = small inline function
    # lambda df: df.groupBy("purchase_date").agg(...) is the same as:
    # def agg_fn(df):
    #   return df.groupBy("purchase_date").agg(...)

    gold_defs = {
        "daily_revenue": {
            "key": "purchase_date",
            "agg": lambda df: df.groupBy("purchase_date").agg(
                F.sum("final_price").alias("CALC_total_revenue"),
                F.count("*").alias("CALC_total_orders")
            )
        },

        "user_metrics": {
            "key": "user_id",
            "agg": lambda df: df.groupBy("user_id").agg(
                F.sum("final_price").alias("CALC_total_spent"),
                F.countDistinct("product_id").alias("CALC_unique_products_bought"),
                F.avg("final_price").alias("CALC_avg_order_value")
            )
        },

        "product_metrics": {
            "key": "product_id",
            "agg": lambda df: df.groupBy("product_id").agg(
                F.sum("final_price").alias("CALC_revenue"),
                F.avg("rating").alias("CALC_avg_rating"),
                F.sum("review_count").alias("CALC_total_reviews"),
                F.count("*").alias("CALC_purchases")
            )
        },

        "shipping_metrics": {
            "key": "location",
            "agg": lambda df: df.groupBy("location").agg(
                F.avg("shipping_time_days").alias("CALC_avg_shipping_time"),
                F.count("*").alias("CALC_order_count"),
                F.sum(F.when(col("is_returned"), 1).otherwise(0)).alias("CALC_return_count")
            )
        },

        "seller_metrics": {
            "key": "seller_id",
            "agg": lambda df: df.groupBy("seller_id").agg(
                F.sum("final_price").alias("CALC_total_revenue"),
                F.avg("seller_rating").alias("CALC_avg_rating"),
                F.countDistinct("user_id").alias("CALC_unique_customers")
            )
        },

        "category_metrics": {
            "key": "category",
            "agg": lambda df: df.groupBy("category").agg(
                F.sum("final_price").alias("CALC_total_revenue"),
                F.avg("rating").alias("CALC_avg_rating"),
                F.count("*").alias("CALC_order_count")
            )
        },

        "delivery_metrics": {
            "key": "delivery_status",
            "agg": lambda df: df.groupBy("delivery_status").agg(
                F.count("*").alias("CALC_count"),
                F.avg("shipping_time_days").alias("CALC_avg_shipping_time")
            )
        },
    }

    for table_name, defn in gold_defs.items():
        key = defn["key"]
        agg_fn = defn["agg"]
        full_table = CONFIG["tables"][table_name]

        # 1) identify dimension values affected by incremental data
        changed_keys = df_incremental.select(key).distinct()

        # 2) full re-aggregation for affected keys only
        df_full_agg = agg_fn(
            df_silver.join(changed_keys, key, "inner")
        )

        # 3) ensure table exists
        if not spark.catalog.tableExists(full_table):
            df_full_agg.write.format("delta").saveAsTable(full_table)
        else:
            # 4) merge upsert
            target = DeltaTable.forName(spark, full_table)
            target.alias("t").merge(
                df_full_agg.alias("s"),
                f"t.{key} = s.{key}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# ===================================
# ORCHESTRATION
# ===================================
def track_run(pipeline_name: str, func, run_id: str):
    """
    Executes a pipeline stage and logs run metadata

    This function wraps pipeline execution with observability tracking, capturing start/end timestamps, row counts, and success/failure status

    Behavior:
    - records start timestamp in control table before execution
    - invokes the provided function (bronze_load, silver_transform, or build_gold)
    - on success:
        - captures returned row counts (rows_in, rows_out)
        - updates run record with end timestamp and SUCCESS status
        - returns function's output dictionary
    - on failure:
        - updates run record with FAILED status
        - re-raises the exception to propagate error upstream

    Args:
        pipeline_name (str): Name of the pipeline stage ("bronze", "silver", "gold")
        func (callable): Function to execute (must accept run_id parameter)
        run_id (str): Unique execution identifier for lineage tracking
    Returns:
        dict: Output from executed function containing row counts and statistics
    Raises:
        Exception: Re-raises any exception encountered during function execution
    """

    # 1) record start
    spark.sql(f"""
        INSERT INTO {CONFIG["tables"]["runs"]}
        VALUES('{pipeline_name}', '{run_id}', current_timestamp(), null, null, null, 'RUNNING')
    """)

    try:
        # 2) execute
        result = func(run_id)

        # 3) update success
        rows_in = result.get("rows_in", 0) if isinstance(result, dict) else 0
        rows_out = result.get("rows_out", 0) if isinstance(result, dict) else 0

        spark.sql(f"""
            UPDATE {CONFIG["tables"]["runs"]}
            SET end_ts = current_timestamp(), status = 'SUCCESS', rows_in = {rows_in}, rows_out = {rows_out}
            WHERE run_id = '{run_id}' AND pipeline = '{pipeline_name}'
        """)

        return result

    except Exception as e:
        # 4) log failure
        spark.sql(f"""
            UPDATE {CONFIG["tables"]["runs"]}
            SET end_ts = current_timestamp(), status = 'FAILED'
            WHERE run_id = '{run_id}' AND pipeline = '{pipeline_name}'
        """)
        raise e

def run_pipeline():
    """
    Executes the full ETL pipeline from Bronze -> Silver -> Gold
    
    Entry point for batch orchestration
    Runs each stage sequentially with observability tracking
    Logs execution metrics to control tables
    Each stage is tracked with run-level observability
    """

    run_id = str(uuid.uuid4())

    bronze_result = track_run("bronze", bronze_load, run_id)
    log_metrics(run_id, {"bronze_rows": bronze_result.get("rows_in", 0)})

    silver_result = track_run(CONFIG["pipelines"]["silver"], silver_transform, run_id)
    log_metrics(run_id, {"silver_rows_out": silver_result.get("rows_out", 0)})

    track_run(CONFIG["pipelines"]["gold"], lambda _: build_gold(), run_id)

# ===================================
# ENTRY
# ===================================
if __name__ == "__main__":
    init_control_tables()
    run_pipeline()