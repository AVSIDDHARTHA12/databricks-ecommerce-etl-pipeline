"""Generate deterministic, intentionally imperfect e-commerce source data."""

import random
from datetime import datetime, timedelta
from typing import Any


CATEGORIES = ("Electronics", "Fashion", "Home & Kitchen", "Books", "Fitness")
CITIES = ("Hyderabad", "Bengaluru", "Mumbai", "Delhi", "Pune", "Chennai", None)
STATUSES = ("COMPLETED", "COMPLETED", "COMPLETED", "CANCELLED", "RETURNED", "PENDING")


def build_records(order_count: int = 50_000, seed: int = 42) -> dict[str, list[dict[str, Any]]]:
    """Build related customer, product, and order records without external data."""
    if order_count < 1:
        raise ValueError("order_count must be greater than zero")

    rng = random.Random(seed)
    customers = [
        {
            "customer_id": f"CUST-{customer_number}",
            "full_name": f"Sample Customer {customer_number}",
            "city": rng.choice(CITIES),
            "signup_date": (datetime(2024, 1, 1) + timedelta(days=rng.randint(0, 700))).date().isoformat(),
            "email": f"customer{customer_number}@example.com",
        }
        for customer_number in range(1001, 8501)
    ]
    products = [
        {
            "product_id": f"PROD-{product_number}",
            "product_name": f"Sample Product {product_number}",
            "category": rng.choice(CATEGORIES),
            "unit_price_raw": f"{rng.uniform(199.0, 14999.0):.2f}",
        }
        for product_number in range(101, 451)
    ]

    customer_ids = [customer["customer_id"] for customer in customers]
    product_by_id = {product["product_id"]: product for product in products}
    orders: list[dict[str, Any]] = []
    start_time = datetime(2025, 1, 1)

    for index in range(1, order_count + 1):
        order_id = f"ORD-{100_000 + index}"
        customer_id = rng.choice(customer_ids) if rng.random() >= 0.03 else None
        product_id = rng.choice(products)["product_id"]
        product = product_by_id[product_id]
        quantity = rng.choice((1, 1, 1, 1, 2, 2, 3, 3, 4))
        if rng.random() < 0.04:
            quantity = rng.choice((0, -1))

        price = float(product["unit_price_raw"])
        if rng.random() < 0.15:
            price_raw = f"₹{price:,.2f}"
        elif rng.random() < 0.01:
            price_raw = "not-a-price"
        else:
            price_raw = product["unit_price_raw"]

        event_time = start_time + timedelta(minutes=rng.randint(0, 525_600))
        if rng.random() < 0.20:
            timestamp_raw = event_time.strftime("%d-%m-%Y %H:%M:%S")
        else:
            timestamp_raw = event_time.strftime("%Y-%m-%d %H:%M:%S")
        if rng.random() < 0.01:
            timestamp_raw = "not-a-timestamp"

        order = {
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": str(quantity),
            "unit_price_raw": price_raw,
            "order_timestamp_raw": timestamp_raw,
            "city": rng.choice(CITIES),
            "order_status": rng.choice(STATUSES),
        }
        orders.append(order)

        if rng.random() < 0.05:
            retry = order.copy()
            if timestamp_raw != "not-a-timestamp":
                retry_time = event_time + timedelta(seconds=30)
                retry["order_timestamp_raw"] = retry_time.strftime("%Y-%m-%d %H:%M:%S")
            orders.append(retry)

    return {"customers": customers, "products": products, "orders": orders}


def generate_raw_data(
    spark: Any,
    output_path: str,
    order_count: int = 50_000,
    seed: int = 42,
) -> dict[str, int]:
    """Write generated records to Spark-readable CSV and JSON Lines directories."""
    from pyspark.sql.types import StringType, StructField, StructType

    records = build_records(order_count=order_count, seed=seed)
    customer_schema = StructType(
        [StructField(field, StringType(), True) for field in ("customer_id", "full_name", "city", "signup_date", "email")]
    )
    product_schema = StructType(
        [StructField(field, StringType(), True) for field in ("product_id", "product_name", "category", "unit_price_raw")]
    )
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

    customer_df = spark.createDataFrame(records["customers"], schema=customer_schema)
    product_df = spark.createDataFrame(records["products"], schema=product_schema)
    order_df = spark.createDataFrame(records["orders"], schema=order_schema)

    customer_df.write.mode("overwrite").json(f"{output_path.rstrip('/')}/customers")
    product_df.write.mode("overwrite").option("header", True).csv(f"{output_path.rstrip('/')}/products")
    order_df.write.mode("overwrite").option("header", True).csv(f"{output_path.rstrip('/')}/orders")

    return {
        "customers": len(records["customers"]),
        "products": len(records["products"]),
        "orders": len(records["orders"]),
    }
