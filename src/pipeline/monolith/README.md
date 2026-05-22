monolith

This folder preserves the original single-file implementation of the pipeline.

medallion_etl_pipeline.py is the complete Bronze -> Silver -> Gold pipeline in one file kept here as a reference and for easy Databricks notebook import during development.

The modular version (the source of truth going forward) lives in src/pipeline/:

MODULE              RESPONSIBILITY
config.py           All configuration and schema definitions
control.py          Run tracking, watermarking, metric logging
bronze.py           Raw ingestion layer
silver.py           Cleaning, validation, deduplication
gold_spark.py       Spark-based business aggregations
orchestrator.py     Entrypoint - runs the full pipeline