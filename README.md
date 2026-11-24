# E-commerce Delta ETL

An end-to-end batch data warehouse project built for Databricks, PySpark, and
Delta Lake. It generates reproducible sample data, ingests three source files,
cleans and quarantines invalid orders, upserts curated orders, builds a star
schema, and publishes daily category KPIs.

## Architecture

```text
Synthetic CSV/JSON
    ├── orders     ──> raw_orders ──> curated_orders ──┐
    ├── customers ──> raw_customers ──> dim_customers ├──> fact_orders
    └── products  ──> raw_products  ──> dim_products  ┘        │
                 invalid orders ──> quarantine_orders          └──> agg_daily_category_kpis
```

The generator uses fake customer names and `example.com` email addresses; it
does not use real customer data. Every run receives a batch ID so raw records
can be traced to their ingestion batch. Curated orders and facts use Delta
`MERGE` keyed by `order_id`, so rerunning a batch does not create duplicate
warehouse orders. Invalid rows are retained in a quarantine table with reasons;
replaying the same quarantined source event does not add another row.

## Run on Databricks

1. Clone this repository into a Databricks Git folder, or import a ZIP of the
   project files into a Workspace folder.
2. Use Databricks Runtime 14.x or later with Apache Spark and Delta Lake.
   The default input path is a Unity Catalog Volume; create it in your workspace
   or change the `raw_path` widget to a writable path supported by your compute.
3. Open and run [`notebooks/ecommerce_etl_pipeline.py`](notebooks/ecommerce_etl_pipeline.py).
   Its `raw_path`, `database`, and `order_count` widgets can be changed before
   running. The default raw path is
   `/Volumes/workspace/default/ecommerce_etl_raw/raw`.
4. Inspect the results:

   ```sql
   SELECT * FROM ecommerce_dw.agg_daily_category_kpis ORDER BY order_date, category;
   SELECT * FROM ecommerce_dw.quarantine_orders LIMIT 20;
   DESCRIBE HISTORY ecommerce_dw.curated_orders;
   ```

`net_revenue` sums `gross_amount` only for `COMPLETED` orders. The generated
source assigns one terminal status to each order and does not provide a
completed-then-returned event history, so a `RETURNED` order is counted
separately rather than subtracted from completed revenue. `return_rate_pct` is
the number of returned orders divided by all fact orders in that date/category
group.

## Databricks validation output

The pipeline was run on Databricks Serverless using the isolated schema
`ecommerce_dw_validation` and raw path
`/Volumes/workspace/default/ecommerce_etl_raw/validation-run-2026-11-24`.
The generated batch contained 7,500 customers, 350 products, and 52,492 order
events. All five notebook cells succeeded. The data-quality checks reported
45,608 fact rows, with zero null or duplicate order IDs, invalid amounts, or
missing customer/product references. The KPI table contained 1,825 rows.

The executed Databricks notebook, including its cell outputs, is available at
[`notebooks/ecommerce_etl_pipeline_output.ipynb`](notebooks/ecommerce_etl_pipeline_output.ipynb).

Sample rows from the Databricks KPI output:

| Order date | Category | Orders | Unique customers | Net revenue | Returned | Return rate |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2025-01-01 | Books | 24 | 24 | 168,677.98 | 7 | 29.17% |
| 2025-01-01 | Electronics | 30 | 30 | 190,531.80 | 5 | 16.67% |
| 2025-01-01 | Fashion | 21 | 21 | 106,002.63 | 6 | 28.57% |
| 2025-01-01 | Fitness | 17 | 17 | 122,354.55 | 2 | 11.76% |
| 2025-01-01 | Home & Kitchen | 29 | 29 | 299,207.10 | 3 | 10.34% |

## Tests

Run the local tests with:

```powershell
python -m unittest discover -s tests -v
```

The generator tests use only the Python standard library. The Spark/Delta
integration tests cover cleansing and quarantine, duplicate-event selection,
batch replay idempotency, late events, and revenue/return KPIs. Run them on
Databricks by opening [`notebooks/run_pipeline_tests.py`](notebooks/run_pipeline_tests.py)
and selecting **Run all**. They are skipped by local discovery when PySpark is
not installed. Local Spark execution requires compatible Java, PySpark, and
Delta Lake versions plus a configured Delta-enabled Spark session.

## Data contracts

| Dataset | Format | Important fields |
| --- | --- | --- |
| Orders | CSV | `order_id`, `customer_id`, `product_id`, quantity, raw price, raw timestamp, status |
| Customers | JSON Lines | `customer_id`, name, city, signup date, email |
| Products | CSV | `product_id`, category, product name, raw unit price |

Valid orders must have an order ID, customer ID, product ID, positive quantity
and price, a parseable timestamp, and a known status. Supported statuses are
`PENDING`, `COMPLETED`, `CANCELLED`, and `RETURNED`. Duplicate order events are
resolved by latest event timestamp, then ingestion timestamp and source path.
Older late events cannot replace a newer curated order.

## Project layout

```text
ecommerce_etl/
  generate_data.py   # Deterministic synthetic source records and Spark file writer
  pipeline.py        # Raw ingestion, cleansing, Delta MERGE, warehouse, KPIs, DQ
notebooks/
  ecommerce_etl_pipeline.py
  ecommerce_etl_pipeline_output.ipynb
  run_pipeline_tests.py
tests/
  test_generate_data.py
  test_pipeline_spark.py
```
