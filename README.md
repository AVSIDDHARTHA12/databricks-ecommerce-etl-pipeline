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
warehouse orders. Invalid rows are retained in a quarantine table with reasons.

## Run on Databricks

1. Import or clone this repository into a Databricks Git folder.
2. Use a Databricks Runtime with Apache Spark and Delta Lake (DBR 14.x or later
   recommended). The notebook uses the Hive metastore and does not require
   Unity Catalog.
3. Open and run [`notebooks/ecommerce_etl_pipeline.py`](notebooks/ecommerce_etl_pipeline.py).
   Its `raw_path`, `database`, and `order_count` widgets can be changed before
   running. The default raw path is `dbfs:/tmp/ecommerce_etl_demo/raw`.
4. Inspect the results:

   ```sql
   SELECT * FROM ecommerce_dw.agg_daily_category_kpis ORDER BY order_date, category;
   SELECT * FROM ecommerce_dw.quarantine_orders LIMIT 20;
   DESCRIBE HISTORY ecommerce_dw.curated_orders;
   ```

The notebook generates fresh input data and runs every stage in order. It
prints the batch ID, counts, and data-quality results. It intentionally does
not include screenshots or claim that a Databricks run has already happened.

## Run the generator tests locally

The generator tests use only the Python standard library:

```powershell
python -m unittest discover -s tests -v
```

Running the Spark pipeline itself requires a Databricks cluster with Delta
Lake. For local Spark execution, install a compatible Java runtime, PySpark,
and Delta Lake version for your Spark release, then configure a Spark session
with Delta extensions and catalog settings.

## Data contracts

| Dataset | Format | Important fields |
| --- | --- | --- |
| Orders | CSV | `order_id`, `customer_id`, `product_id`, quantity, raw price, raw timestamp, status |
| Customers | JSON Lines | `customer_id`, name, city, signup date, email |
| Products | CSV | `product_id`, category, product name, raw unit price |

Valid orders must have an order ID, customer ID, product ID, positive quantity
and price, a parseable timestamp, and a known status. Supported statuses are
`PENDING`, `COMPLETED`, `CANCELLED`, and `RETURNED`. Duplicate order events are
resolved by latest event timestamp, then ingestion timestamp. Older late
events cannot replace a newer curated order.

## Project layout

```text
ecommerce_etl/
  generate_data.py   # Deterministic synthetic source records and Spark file writer
  pipeline.py        # Raw ingestion, cleansing, Delta MERGE, warehouse, KPIs, DQ
notebooks/
  ecommerce_etl_pipeline.py
tests/
  test_generate_data.py
```
