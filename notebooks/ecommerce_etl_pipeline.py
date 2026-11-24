# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce Delta ETL
# MAGIC Generates synthetic source data, runs the batch pipeline, and displays the output tables.

# COMMAND ----------

from pathlib import Path
import sys
import uuid

project_root = next(
    (parent for parent in (Path.cwd(), *Path.cwd().parents) if (parent / "pyproject.toml").is_file()),
    None,
)
if project_root is None:
    raise FileNotFoundError(
        "Could not locate pyproject.toml. Open this notebook from the project Git folder."
    )
sys.path.insert(0, str(project_root))

from ecommerce_etl.generate_data import generate_raw_data
from ecommerce_etl.pipeline import run_pipeline

# COMMAND ----------

# DBTITLE 1,Configure widgets and generate raw data
dbutils.widgets.text("raw_path", "/Volumes/workspace/default/ecommerce_etl_raw/raw")
dbutils.widgets.text("database", "ecommerce_dw")
dbutils.widgets.text("order_count", "50000")

raw_path = dbutils.widgets.get("raw_path").rstrip("/")
database = dbutils.widgets.get("database")
order_count = int(dbutils.widgets.get("order_count"))
batch_id = str(uuid.uuid4())

generated_counts = generate_raw_data(spark, raw_path, order_count=order_count)
print(f"Generated source records: {generated_counts}")
print(f"Processing batch: {batch_id}")

# COMMAND ----------

dq_results = run_pipeline(spark, raw_path, database, batch_id)
print(f"Data-quality checks passed: {dq_results}")

# COMMAND ----------

display(spark.table(f"{database}.agg_daily_category_kpis").orderBy("order_date", "category"))

# COMMAND ----------

display(spark.table(f"{database}.quarantine_orders").filter(f"batch_id = '{batch_id}'").limit(50))
