import unittest
import uuid
from datetime import date, datetime
from importlib.util import find_spec

from ecommerce_etl.pipeline import build_kpis, curate_orders, prepare_order_batch


HAS_PYSPARK = find_spec("pyspark") is not None


@unittest.skipUnless(HAS_PYSPARK, "Run these integration tests on Databricks or with PySpark installed.")
class PipelineSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    def setUp(self):
        self.database = f"ecommerce_test_{uuid.uuid4().hex[:12]}"
        self.spark.sql(f"CREATE DATABASE `{self.database}`")

    def tearDown(self):
        self.spark.sql(f"DROP DATABASE IF EXISTS `{self.database}` CASCADE")

    def _raw_orders(self, rows):
        from pyspark.sql.types import StringType, StructField, StructType, TimestampType

        schema = StructType(
            [
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", StringType(), True),
                StructField("unit_price_raw", StringType(), True),
                StructField("order_timestamp_raw", StringType(), True),
                StructField("city", StringType(), True),
                StructField("order_status", StringType(), True),
                StructField("batch_id", StringType(), True),
                StructField("ingested_at", TimestampType(), True),
                StructField("source_file", StringType(), True),
            ]
        )
        return self.spark.createDataFrame(rows, schema=schema)

    def test_cleans_currency_parses_both_date_formats_and_quarantines_bad_rows(self):
        raw = self._raw_orders(
            [
                (
                    "ORD-1",
                    "CUST-1",
                    "PROD-1",
                    "2",
                    "₹1,200.50",
                    "2025-03-31 12:00:00",
                    " Mumbai ",
                    "completed",
                    "batch-1",
                    datetime(2026, 1, 1, 10, 0),
                    "orders.csv",
                ),
                (
                    "ORD-1",
                    "CUST-1",
                    "PROD-1",
                    "2",
                    "₹1,200.50",
                    "01-04-2025 12:00:00",
                    "Mumbai",
                    "returned",
                    "batch-1",
                    datetime(2026, 1, 1, 10, 0),
                    "orders.csv",
                ),
                (
                    "ORD-ZERO",
                    "CUST-2",
                    "PROD-1",
                    "0",
                    "25.00",
                    "2025-04-01 12:00:00",
                    "Delhi",
                    "completed",
                    "batch-1",
                    datetime(2026, 1, 1, 10, 0),
                    "orders.csv",
                ),
                (
                    "ORD-BAD-TS",
                    "CUST-3",
                    "PROD-1",
                    "1",
                    "25.00",
                    "not-a-timestamp",
                    "Pune",
                    "completed",
                    "batch-1",
                    datetime(2026, 1, 1, 10, 0),
                    "orders.csv",
                ),
            ]
        )

        valid, quarantine = prepare_order_batch(raw)

        self.assertEqual(valid.count(), 1)
        order = valid.select("unit_price", "order_ts", "order_date", "order_status", "city").first()
        self.assertEqual(order.unit_price, 1200.5)
        self.assertEqual(order.order_ts.date(), date(2025, 4, 1))
        self.assertEqual(order.order_date, date(2025, 4, 1))
        self.assertEqual(order.order_status, "RETURNED")
        self.assertEqual(order.city, "Mumbai")

        reasons = {row.order_id: row.quarantine_reason for row in quarantine.select(
            "order_id", "quarantine_reason"
        ).collect()}
        self.assertIn("invalid quantity", reasons["ORD-ZERO"])
        self.assertIn("invalid order timestamp", reasons["ORD-BAD-TS"])

    def test_replaying_batch_is_idempotent_and_late_event_does_not_replace_newer_order(self):
        raw = self._raw_orders(
            [
                (
                    "ORD-1",
                    "CUST-1",
                    "PROD-1",
                    "1",
                    "100.00",
                    "2025-04-02 12:00:00",
                    "Mumbai",
                    "COMPLETED",
                    "batch-new",
                    datetime(2026, 1, 1, 10, 0),
                    "orders.csv",
                ),
                (
                    "ORD-1",
                    "CUST-1",
                    "PROD-1",
                    "1",
                    "100.00",
                    "2025-04-01 12:00:00",
                    "Mumbai",
                    "RETURNED",
                    "batch-late",
                    datetime(2026, 1, 2, 10, 0),
                    "orders.csv",
                ),
                (
                    "ORD-BAD",
                    "CUST-2",
                    "PROD-1",
                    "0",
                    "100.00",
                    "2025-04-02 12:00:00",
                    "Delhi",
                    "COMPLETED",
                    "batch-late",
                    datetime(2026, 1, 2, 10, 0),
                    "orders.csv",
                ),
            ]
        )
        raw.write.format("delta").saveAsTable(f"{self.database}.raw_orders")

        curate_orders(self.spark, self.database, "batch-new")
        curate_orders(self.spark, self.database, "batch-late")
        curate_orders(self.spark, self.database, "batch-late")

        curated = self.spark.table(f"{self.database}.curated_orders")
        quarantine = self.spark.table(f"{self.database}.quarantine_orders")
        self.assertEqual(curated.count(), 1)
        self.assertEqual(curated.first().order_status, "COMPLETED")
        self.assertEqual(quarantine.count(), 1)

    def test_returned_orders_are_counted_but_not_subtracted_from_completed_revenue(self):
        from pyspark.sql.types import (
            DateType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        product_schema = StructType(
            [
                StructField("product_sk", LongType(), False),
                StructField("category", StringType(), False),
            ]
        )
        products = self.spark.createDataFrame([(1, "Books")], schema=product_schema)
        products.write.format("delta").saveAsTable(f"{self.database}.dim_products")

        fact_schema = StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("customer_sk", LongType(), False),
                StructField("product_sk", LongType(), False),
                StructField("order_date", DateType(), False),
                StructField("order_ts", TimestampType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", DoubleType(), False),
                StructField("gross_amount", DoubleType(), False),
                StructField("order_status", StringType(), False),
            ]
        )
        facts = self.spark.createDataFrame(
            [
                ("ORD-C", 1, 1, date(2025, 1, 1), datetime(2025, 1, 1, 10), 1, 100.0, 100.0, "COMPLETED"),
                ("ORD-R", 2, 1, date(2025, 1, 1), datetime(2025, 1, 1, 11), 1, 50.0, 50.0, "RETURNED"),
            ],
            schema=fact_schema,
        )
        facts.write.format("delta").saveAsTable(f"{self.database}.fact_orders")

        build_kpis(self.spark, self.database)

        result = self.spark.table(f"{self.database}.agg_daily_category_kpis").first()
        self.assertEqual(result.net_revenue, 100.0)
        self.assertEqual(result.returned_orders, 1)
        self.assertEqual(result.return_rate_pct, 50.0)


if __name__ == "__main__":
    unittest.main()
