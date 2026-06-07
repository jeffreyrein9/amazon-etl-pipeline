# Amazon E-Commerce ETL Pipeline

Incremental batch pipeline built on Apache Spark and Delta Lake, implementing a
Medallion Architecture (Bronze->Silver->Gold) for Amazon e-commerce data.

## Architecture

```text
Raw CSV (Databricks Volumes)
            |
            |
            |
Bronze - Append-only raw ingestion. No transformations.
Adds ingest_ts, source_file, run_id metadata.
            |
            | watermark
            |
Silver - Type casting, null handling, standardization.
Validation + reject logging. Hash-based deduplication.  -------------------------
Idempotent merge into clean Delta table.                                        |
            |                                                                   |
            |                                                                   |
            |                                                                   |
Gold Spark - Incremental business aggregations via Spark.           Gold DBT - Curated semantic models via dbt-databricks.                          
Only recomputes dimension groups touched by new data.               (business logic, joins, reporting-ready)
7 analytical tables (revenue, users, products, etc.)
```

## Control Plane
Every run is tracked in Delta control tables:
- control.pipeline_runs — start/end timestamps, row counts, SUCCESS/FAILED status
- control.pipeline_metrics — flexible key/value metrics per run
- control.pipeline_rejects — full row payload of records that failed validation

## Tech Stack

```text
| Layer                     | Technology                        |
|---------------------------|-----------------------------------|
| Compute                   | Apache Spark (Databricks Runtime) |
| Storage                   | Delta Lake                        |
| Orchestration             | Databricks Jobs                   |
| Transformation            | dbt-databricks                    |
| Language                  | Python 3                          |
```

## Dashboard

[View on Tableau Public](https://public.tableau.com/app/profile/jeffrey.rein/viz/AmazonE-CommerceRevenueOverview/Dashboard1)

Interactive dashboard analyzing FY2026 vs FY2025 Amazon e-commerce revenue built on a full end-to-end data pipeline. Features
KPI cards with sparklines, revenue by category and subcategory, and a violin plot showing order value distribution by category.

## Project Structure

```text
amazon-etl-pipeline/
│
├── src/
│   └── pipeline/
│       ├── config.py          # All configuration: paths, schemas, table names
│       ├── control.py         # Run tracking, watermarking, metric logging
│       ├── bronze.py          # Raw ingestion layer
│       ├── silver.py          # Cleaning, validation, deduplication
│       ├── gold_spark.py      # Spark-based business aggregations
│       ├── orchestrator.py    # Entry point — runs the full pipeline
│       └── monolith/
│           └── medallion_etl_pipeline.py  # Original single-file version (reference)
│
├── notebooks/                 # Databricks notebooks for exploration
│   └── violin_plot.py         # Matplotlib violin plot for order value distribution
├── dbt/                       # dbt project (coming soon)
├── docs/                      # Architecture diagrams and notes
├── tests/                     # Unit tests
│
├── .gitignore
├── requirements.txt
└── README.md
```

## Gold Layer Tables (Spark)

```text
| Table                     | Grain             | Key Metrics                                   |
|---------------------------|-------------------|-----------------------------------------------|
| gold.daily_revenue        | purchase_date     | total revenue, order count                    |
| gold.user_metrics         | user_id           | total spent, unique products, avg order value |
| gold.product_metrics      | product_id        | revenue, avg rating, total reviews, purchases |
| gold.shipping_metrics     | location          | avg shipping time, order count, return count  |
| gold.seller_metrics       | seller_id         | total revenue, avg rating, unique customers   |
| gold.category_metrics     | category          | total revenue, avg rating, order count        |
| gold.delivery_metrics     | delivery_status   | count, avg shipping time                      |
```

## Data Source

[Amazon E-Commerce Dataset — Kaggle](https://www.kaggle.com/datasets/sharmajicoder/amazon-e-commerce)

~1M rows of Amazon transaction data including products, pricing, sellers,
shipping, ratings, and customer behavior.

Raw data is stored in Databricks Volumes and is not committed to this repository.

## Pipeline Design Principles

- Incremental Processing - watermark-based filtering so each run only touches new data
- Idempotent - safe to re-run without creating duplicates (row hashing + Delta merge)
- Observability - every run logged with row counts, timing, and status
- Separation of Concerns - each layer is independently testable and replaceable