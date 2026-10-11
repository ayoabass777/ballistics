"""Evaluation diagnostics for feature selection and model validation."""

import warnings

# The engine passes psycopg2 connections to pandas.read_sql. pandas works with
# them but warns on every query that only SQLAlchemy is "tested". Silence that
# one message so real warnings stay visible (2026-10-03).
warnings.filterwarnings(
    "ignore",
    message=r"pandas only supports SQLAlchemy connectable",
    category=UserWarning,
)
