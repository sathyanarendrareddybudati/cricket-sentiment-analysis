"""Compatibility DAG entry point for Airflow container runs.

This file provides the path name expected by the container-based DAG loader
while reusing the real DAG definition in cricket_pipeline.py.
"""

from cricket_pipeline import dag  # noqa: F401
