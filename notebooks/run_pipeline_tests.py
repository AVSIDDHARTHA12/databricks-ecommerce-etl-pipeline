# Databricks notebook source
# MAGIC %md
# MAGIC # Run pipeline integration tests
# MAGIC Runs the Spark/Delta tests against the active Databricks session.

# COMMAND ----------

from pathlib import Path
import sys
import unittest

project_root = next(
    (parent for parent in (Path.cwd(), *Path.cwd().parents) if (parent / "pyproject.toml").is_file()),
    None,
)
if project_root is None:
    raise FileNotFoundError("Open this notebook from the project folder.")

sys.path.insert(0, str(project_root))
suite = unittest.defaultTestLoader.discover(
    str(project_root / "tests"),
    pattern="test_*.py",
    top_level_dir=str(project_root),
)
result = unittest.TextTestRunner(verbosity=2).run(suite)
if not result.wasSuccessful():
    raise AssertionError("Pipeline tests failed; see test output above.")
