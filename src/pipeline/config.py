"""
Pipeline configuration.

All environment-specific paths, table names, schema definitions,
and data quality rules live here. Update this file when adapting
the pipeline to a new dataset or environment.
"""

CONFIG = {
    "pipeline_name": "amazon_etl",
    "pipelines": {
        "silver": "amazon_silver",
        "gold":   "amazon_gold"
    },
    "source": "/Volumes/workspace/default/amazon_ecommerce/amazon_ecommerce_1M.csv",
    "paths": {
        "bronze": "/Volumes/workspace/default/amazon_ecommerce/bronze",
        "silver": "/Volumes/workspace/default/amazon_ecommerce/silver",
        "gold":   "/Volumes/workspace/default/amazon_ecommerce/gold",
    },
    "tables": {
        "bronze":            "bronze.amazon_raw",
        "silver":            "silver.amazon_clean",
        "daily_revenue":     "gold.daily_revenue",
        "user_metrics":      "gold.user_metrics",
        "product_metrics":   "gold.product_metrics",
        "shipping_metrics":  "gold.shipping_metrics",
        "seller_metrics":    "gold.seller_metrics",
        "category_metrics":  "gold.category_metrics",
        "delivery_metrics":  "gold.delivery_metrics",
        "runs":              "control.pipeline_runs",
        "metrics":           "control.pipeline_metrics",
        "rejects":           "control.pipeline_rejects",
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
            "user_id":             "string",
            "product_id":          "string",
            "category":            "string",
            "subcategory":         "string",
            "brand":               "string",
            "price":               "decimal(15,2)",
            "discount":            "decimal(15,2)",
            "final_price":         "decimal(15,2)",
            "rating":              "double",
            "review_count":        "integer",
            "stock":               "integer",
            "seller_id":           "string",
            "seller_rating":       "double",
            "purchase_date":       "date",
            "shipping_time_days":  "integer",
            "location":            "string",
            "device":              "string",
            "payment_method":      "string",
            "is_returned":         "boolean",
            "delivery_status":     "string"
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
                "discount":     0.0,
                "review_count": 0
            },
            "categorical": {
                "category":       "unknown",
                "subcategory":    "unknown",
                "brand":          "unknown",
                "location":       "unknown",
                "device":         "unknown",
                "payment_method": "unknown",
                "delivery_status":"unknown"
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