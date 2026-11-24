import unittest

from ecommerce_etl.generate_data import build_records


class BuildRecordsTests(unittest.TestCase):
    def test_records_are_reproducible_and_refer_to_generated_catalogs(self):
        first = build_records(order_count=100, seed=7)
        second = build_records(order_count=100, seed=7)

        self.assertEqual(first, second)
        self.assertEqual(len(first["customers"]), 7500)
        self.assertEqual(len(first["products"]), 350)
        self.assertGreaterEqual(len(first["orders"]), 100)

        customer_ids = {row["customer_id"] for row in first["customers"]}
        product_ids = {row["product_id"] for row in first["products"]}
        for order in first["orders"]:
            self.assertTrue(order["product_id"] in product_ids)
            if order["customer_id"] is not None:
                self.assertIn(order["customer_id"], customer_ids)

    def test_invalid_order_count_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "greater than zero"):
            build_records(order_count=0)


if __name__ == "__main__":
    unittest.main()
