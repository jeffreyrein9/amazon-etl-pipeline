"""
Orchestrator: pipeline entry point.

This is the only file you run. It wires together all pipeline stages
in order (bronze -> silver -> gold) and handles run-level observability.

Usage in Databricks:
    Import and call run_pipeline() from a notebook, or attach this file
    as a job task and point to run_pipeline as the entry point.

Usage locally (for testing):
    python -m src.pipeline.orchestrator
"""

import uuid
import logging

from src.pipeline.config import CONFIG
from src.pipeline.control import init_control_tables, track_run, log_metrics
from src.pipeline.bronze import bronze_load
from src.pipeline.silver import silver_transform
from src.pipeline.gold_spark import build_gold

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pipeline")


def run_pipeline():
    """
    Executes the full ETL pipeline: Bronze -> Silver -> Gold.

    Each stage is wrapped with run-level tracking (start/end timestamps,
    row counts, SUCCESS/FAILED status) written to control tables.
    Metrics are logged separately for downstream monitoring.
    """
    run_id = str(uuid.uuid4())
    logger.info(f"Pipeline starting — run_id: {run_id}")

    # Bronze
    bronze_result = track_run("bronze", bronze_load, run_id)
    log_metrics(run_id, {"bronze_rows": bronze_result.get("rows_in", 0)})

    # Silver
    silver_result = track_run(CONFIG["pipelines"]["silver"], silver_transform, run_id)
    log_metrics(run_id, {
        "silver_rows_out":      silver_result.get("rows_out", 0),
        "silver_rows_rejected": silver_result.get("rows_rejected", 0),
    })

    # Gold (Spark aggregations)
    track_run(CONFIG["pipelines"]["gold"], lambda _: build_gold(), run_id)

    logger.info(f"Pipeline complete — run_id: {run_id}")


if __name__ == "__main__":
    init_control_tables()
    run_pipeline()
