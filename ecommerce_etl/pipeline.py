"""Databricks batch pipeline: raw Delta, curated orders, star schema, and KPIs."""

from typing import Any


def _qualified(database: str, table: str) -> str:
    safe_database = database.replace("`", "``")
    return f"`{safe_database}`.`{table}`"


def ingest_raw(spark: Any, raw_path: str, database: str, batch_id: str) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    database_sql = f"`{database.replace('`', '``')}`"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_sql}")

    order_schema = StructType(
        [
            StructField(field, StringType(), True)
            for field in (
                "order_id",
                "customer_id",
                "product_id",
                "quantity",
                "unit_price_raw",
                "order_timestamp_raw",
                "city",
                "order_status",
            )
        ]
    )
    customer_schema = StructType(
        [StructField(field, StringType(), True) for field in ("customer_id", "full_name", "city", "signup_date", "email")]
    )
    product_schema = StructType(
        [StructField(field, StringType(), True) for field in ("product_id", "product_name", "category", "unit_price_raw")]
    )

    sources = (
        ("orders", "raw_orders", order_schema, "csv"),
        ("customers", "raw_customers", customer_schema, "json"),
        ("products", "raw_products", product_schema, "csv"),
    )
    for folder, table, schema, file_format in sources:
        reader = spark.read.schema(schema)
        if file_format == "csv":
            reader = reader.option("header", True)
        source = reader.format(file_format).load(f"{raw_path.rstrip('/')}/{folder}")
        audited = (
            source.withColumn("batch_id", F.lit(batch_id))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("source_file", F.expr("_metadata.file_path"))
        )
        audited.write.format("delta").mode("append").saveAsTable(_qualified(database, table))


def curate_orders(spark: Any, database: str, batch_id: str) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    raw = spark.table(_qualified(database, "raw_orders")).filter(F.col("batch_id") == batch_id)
    cleaned = (
        raw.withColumn(
            "unit_price_clean",
            F.regexp_replace(F.col("unit_price_raw"), r"₹|\$|,|\s", ""),
        )
        .withColumn("unit_price", F.expr("try_cast(unit_price_clean AS DOUBLE)"))
        .withColumn("quantity_int", F.expr("try_cast(quantity AS INT)"))
        .withColumn(
            "order_ts",
            F.coalesce(
                F.expr("try_to_timestamp(order_timestamp_raw, 'yyyy-MM-dd HH:mm:ss')"),
                F.expr("try_to_timestamp(order_timestamp_raw, 'dd-MM-yyyy HH:mm:ss')"),
            ),
        )
        .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
        .withColumn("city", F.coalesce(F.trim(F.col("city")), F.lit("UNKNOWN")))
    )
    valid = F.coalesce(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.col("quantity_int") > 0)
        & (F.col("unit_price") > 0)
        & F.col("order_ts").isNotNull()
        & F.col("order_status").isin("PENDING", "COMPLETED", "CANCELLED", "RETURNED"),
        F.lit(False),
    )

    reasons = F.concat_ws(
        "; ",
        F.when(F.col("order_id").isNull(), F.lit("missing order_id")),
        F.when(F.col("customer_id").isNull(), F.lit("missing customer_id")),
        F.when(F.col("product_id").isNull(), F.lit("missing product_id")),
        F.when(F.col("quantity_int").isNull() | (F.col("quantity_int") <= 0), F.lit("invalid quantity")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), F.lit("invalid unit price")),
        F.when(F.col("order_ts").isNull(), F.lit("invalid order timestamp")),
        F.when(F.col("order_status").isNull(), F.lit("missing order status")),
        F.when(
            F.col("order_status").isNotNull()
            & ~F.col("order_status").isin("PENDING", "COMPLETED", "CANCELLED", "RETURNED"),
            F.lit("unsupported order status"),
        ),
    )
    quarantine = (
        cleaned.filter(~valid)
        .withColumn("quarantine_reason", reasons)
        .withColumn("quarantined_at", F.current_timestamp())
    )
    quarantine.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        _qualified(database, "quarantine_orders")
    )

    order_window = Window.partitionBy("order_id").orderBy(
        F.col("order_ts").desc(), F.col("ingested_at").desc(), F.col("source_file").desc()
    )
    deduped = (
        cleaned.filter(valid)
        .withColumn("_row_number", F.row_number().over(order_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number", "unit_price_raw", "unit_price_clean", "order_timestamp_raw", "quantity")
        .withColumnRenamed("quantity_int", "quantity")
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("processed_at", F.current_timestamp())
    )

    target_name = _qualified(database, "curated_orders")
    if not spark.catalog.tableExists(f"{database}.curated_orders"):
        deduped.write.format("delta").mode("overwrite").partitionBy("order_date").saveAsTable(target_name)
        return

    (
        DeltaTable.forName(spark, f"{database}.curated_orders")
        .alias("tgt")
        .merge(deduped.alias("src"), "tgt.order_id = src.order_id")
        .whenMatchedUpdateAll(condition="src.order_ts >= tgt.order_ts")
        .whenNotMatchedInsertAll()
        .execute()
    )


def build_warehouse(spark: Any, database: str) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    customers = _qualified(database, "dim_customers")
    products = _qualified(database, "dim_products")
    facts = _qualified(database, "fact_orders")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {customers} (
            customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
            customer_id STRING,
            full_name STRING,
            primary_city STRING,
            signup_date DATE,
            email STRING,
            last_updated TIMESTAMP
        ) USING DELTA
        """
    )
    customer_window = Window.partitionBy("customer_id").orderBy(F.col("ingested_at").desc())
    customer_source = (
        spark.table(_qualified(database, "raw_customers"))
        .filter(F.col("customer_id").isNotNull())
        .withColumn("_row_number", F.row_number().over(customer_window))
        .filter(F.col("_row_number") == 1)
        .select(
            "customer_id",
            "full_name",
            F.coalesce(F.trim(F.col("city")), F.lit("UNKNOWN")).alias("primary_city"),
            F.to_date("signup_date").alias("signup_date"),
            "email",
            F.current_timestamp().alias("last_updated"),
        )
    )
    (
        DeltaTable.forName(spark, f"{database}.dim_customers")
        .alias("tgt")
        .merge(customer_source.alias("src"), "tgt.customer_id = src.customer_id")
        .whenMatchedUpdate(
            set={
                "full_name": "src.full_name",
                "primary_city": "src.primary_city",
                "signup_date": "src.signup_date",
                "email": "src.email",
                "last_updated": "src.last_updated",
            }
        )
        .whenNotMatchedInsert(
            values={
                "customer_id": "src.customer_id",
                "full_name": "src.full_name",
                "primary_city": "src.primary_city",
                "signup_date": "src.signup_date",
                "email": "src.email",
                "last_updated": "src.last_updated",
            }
        )
        .execute()
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {products} (
            product_sk BIGINT GENERATED ALWAYS AS IDENTITY,
            product_id STRING,
            product_name STRING,
            category STRING,
            latest_unit_price DOUBLE,
            last_updated TIMESTAMP
        ) USING DELTA
        """
    )
    product_window = Window.partitionBy("product_id").orderBy(F.col("ingested_at").desc())
    product_source = (
        spark.table(_qualified(database, "raw_products"))
        .filter(F.col("product_id").isNotNull())
        .withColumn("_row_number", F.row_number().over(product_window))
        .filter(F.col("_row_number") == 1)
        .withColumn("unit_price_clean", F.regexp_replace(F.col("unit_price_raw"), r"₹|\$|,|\s", ""))
        .select(
            "product_id",
            "product_name",
            "category",
            F.expr("try_cast(unit_price_clean AS DOUBLE)").alias("latest_unit_price"),
            F.current_timestamp().alias("last_updated"),
        )
        .filter(F.col("latest_unit_price") > 0)
    )
    (
        DeltaTable.forName(spark, f"{database}.dim_products")
        .alias("tgt")
        .merge(product_source.alias("src"), "tgt.product_id = src.product_id")
        .whenMatchedUpdate(
            set={
                "product_name": "src.product_name",
                "category": "src.category",
                "latest_unit_price": "src.latest_unit_price",
                "last_updated": "src.last_updated",
            }
        )
        .whenNotMatchedInsert(
            values={
                "product_id": "src.product_id",
                "product_name": "src.product_name",
                "category": "src.category",
                "latest_unit_price": "src.latest_unit_price",
                "last_updated": "src.last_updated",
            }
        )
        .execute()
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {facts} (
            order_id STRING,
            customer_sk BIGINT,
            product_sk BIGINT,
            order_date DATE,
            order_ts TIMESTAMP,
            quantity INT,
            unit_price DOUBLE,
            gross_amount DOUBLE,
            order_status STRING
        ) USING DELTA
        PARTITIONED BY (order_date)
        """
    )
    fact_source = (
        spark.table(_qualified(database, "curated_orders")).alias("o")
        .join(
            spark.table(_qualified(database, "dim_customers")).alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "inner",
        )
        .join(
            spark.table(_qualified(database, "dim_products")).alias("p"),
            F.col("o.product_id") == F.col("p.product_id"),
            "inner",
        )
        .select(
            F.col("o.order_id").alias("order_id"),
            F.col("c.customer_sk").alias("customer_sk"),
            F.col("p.product_sk").alias("product_sk"),
            F.col("o.order_date").alias("order_date"),
            F.col("o.order_ts").alias("order_ts"),
            F.col("o.quantity").cast("int").alias("quantity"),
            F.col("o.unit_price").alias("unit_price"),
            F.col("o.gross_amount").alias("gross_amount"),
            F.col("o.order_status").alias("order_status"),
        )
    )
    (
        DeltaTable.forName(spark, f"{database}.fact_orders")
        .alias("tgt")
        .merge(fact_source.alias("src"), "tgt.order_id = src.order_id")
        .whenMatchedUpdateAll(condition="src.order_ts >= tgt.order_ts")
        .whenNotMatchedInsertAll()
        .execute()
    )


def build_kpis(spark: Any, database: str) -> None:
    kpis = spark.sql(
        f"""
        SELECT
            f.order_date,
            p.category,
            COUNT(DISTINCT f.order_id) AS total_orders,
            COUNT(DISTINCT f.customer_sk) AS unique_customers,
            SUM(
                CASE
                    WHEN f.order_status = 'COMPLETED' THEN f.gross_amount
                    WHEN f.order_status = 'RETURNED' THEN -f.gross_amount
                    ELSE 0
                END
            ) AS net_revenue,
            SUM(CASE WHEN f.order_status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_orders,
            ROUND(
                SUM(CASE WHEN f.order_status = 'RETURNED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                2
            ) AS return_rate_pct
        FROM {_qualified(database, 'fact_orders')} f
        JOIN {_qualified(database, 'dim_products')} p ON f.product_sk = p.product_sk
        GROUP BY f.order_date, p.category
        """
    )
    kpis.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        _qualified(database, "agg_daily_category_kpis")
    )


def run_dq_checks(spark: Any, database: str) -> dict[str, int]:
    from pyspark.sql import functions as F

    facts = spark.table(_qualified(database, "fact_orders"))
    total_rows = facts.count()
    null_order_ids = facts.filter(F.col("order_id").isNull()).count()
    duplicate_order_ids = total_rows - facts.select("order_id").distinct().count()
    invalid_amounts = facts.filter(
        F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
        | F.col("gross_amount").isNull()
        | (F.col("gross_amount") <= 0)
    ).count()
    missing_customers = facts.join(
        spark.table(_qualified(database, "dim_customers")).select("customer_sk"),
        "customer_sk",
        "left_anti",
    ).count()
    missing_products = facts.join(
        spark.table(_qualified(database, "dim_products")).select("product_sk"),
        "product_sk",
        "left_anti",
    ).count()
    checks = {
        "fact_rows": total_rows,
        "null_order_ids": null_order_ids,
        "duplicate_order_ids": duplicate_order_ids,
        "invalid_amounts": invalid_amounts,
        "missing_customers": missing_customers,
        "missing_products": missing_products,
    }
    failures = [name for name, count in checks.items() if name != "fact_rows" and count != 0]
    if total_rows == 0:
        failures.append("fact table is empty")
    if failures:
        raise ValueError(f"Data quality checks failed for {database}.fact_orders: {failures}; counts={checks}")
    return checks


def run_pipeline(spark: Any, raw_path: str, database: str, batch_id: str) -> dict[str, int]:
    """Run all batch stages and return DQ counts for the warehouse fact."""
    ingest_raw(spark, raw_path, database, batch_id)
    curate_orders(spark, database, batch_id)
    build_warehouse(spark, database)
    build_kpis(spark, database)
    return run_dq_checks(spark, database)
