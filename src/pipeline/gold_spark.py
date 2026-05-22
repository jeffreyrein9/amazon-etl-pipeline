"""
Gold layer (Spark): incremental business aggregations via Apache Spark.

Computes analytical aggregates from silver data using Spark + Delta Lake.
Only dimension keys affected by new data are recomputed, keeping costs low.

Note on naming:
    This file is intentionally named gold_spark.py to distinguish it from
    the future gold_dbt.py layer, which will handle curated/semantic models
    via dbt once the Spark pipeline is stable.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from delta.tables import DeltaTable  # type: ignore

from src.pipeline.config import CONFIG
from src.pipeline.control import get_last_success_ts

logger = logging.getLogger("pipeline")


def _spark():
    """Returns the active Spark session (provided by Databricks at runtime)."""
    return SparkSession.getActiveSession()


# ===================================
# GOLD AGGREGATION DEFINITIONS
# ===================================

# Each entry defines one gold table:
#   key : the dimension column used for incremental scoping and merge
#   agg : a function that takes a DataFrame and returns an aggregated DataFrame
#
# To add a new gold table, add an entry here — no other code needs to change.

GOLD_DEFS = {
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


# ===================================
# GOLD BUILD
# ===================================

def build_gold() -> None:
    """
    Orchestrates incremental computation of all gold aggregation tables.

    Uses watermark logic to detect new silver records. If none exist,
    exits early. Otherwise delegates to write_gold_tables() for
    partial recomputation of only affected dimension groups.

    Returns:
        None
    """
    spark = _spark()

    gold_last_ts = get_last_success_ts(CONFIG["pipelines"]["gold"])
    df_silver = spark.table(CONFIG["tables"]["silver"])

    df_incremental = df_silver.filter(
        col("ingest_ts") > F.lit(gold_last_ts)
    )

    if df_incremental.limit(1).count() == 0:
        logger.info("Gold: no new changes to process.")
        return None

    write_gold_tables(df_silver, df_incremental)


def write_gold_tables(df_silver: DataFrame, df_incremental: DataFrame) -> None:
    """
    Incrementally updates all gold aggregation tables.

    Strategy per table:
        1. Identify dimension values touched by new incremental data
        2. Re-aggregate the full silver dataset for only those keys
        3. Merge results into the gold Delta table (upsert)

    This approach is both correct (full group recalculation) and
    efficient (scoped to changed keys only).

    Args:
        df_silver     (DataFrame): Full silver dataset.
        df_incremental(DataFrame): Only newly ingested silver records.
    """
    spark = _spark()

    for table_name, defn in GOLD_DEFS.items():
        key    = defn["key"]
        agg_fn = defn["agg"]
        full_table = CONFIG["tables"][table_name]

        # 1) find which dimension values were affected
        changed_keys = df_incremental.select(key).distinct()

        # 2) re-aggregate only affected groups from full silver
        df_full_agg = agg_fn(
            df_silver.join(changed_keys, key, "inner")
        )

        # 3) create table on first run, then merge
        if not spark.catalog.tableExists(full_table):
            df_full_agg.write.format("delta").saveAsTable(full_table)
        else:
            target = DeltaTable.forName(spark, full_table)
            target.alias("t").merge(
                df_full_agg.alias("s"),
                f"t.{key} = s.{key}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.info(f"Gold table updated: {full_table}")