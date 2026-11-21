"""Reusable data quality checks.

A Check compares one number against a threshold. The number is either an aggregate Column over the DataFrame
(all of them are computed in a single pass) or a value computed beforehand (e.g. quarantine ratio).
Critical checks that fail make enforce() raise; non-critical ones are only reported.

    results = run_checks(df, [not_null("id"), unique("id"), max_null_ratio("title", 0.01)])
    log_results(results, "my_job", "batch-1")
    enforce(results)
"""
import json
import operator
from dataclasses import dataclass, replace
from functools import reduce
from typing import Any, List

from pyspark.sql import Column
from pyspark.sql import functions as F

COMPARISONS = {"<=": operator.le, ">=": operator.ge}


class QualityCheckError(Exception):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    value: Any          # aggregate Column, or a precomputed number
    comparison: str     # "<=" or ">="
    threshold: float
    critical: bool = True


@dataclass(frozen=True)
class CheckResult:
    name: str
    value: float
    comparison: str
    threshold: float
    critical: bool
    passed: bool


# ---------- check factories ----------

def not_null(column, critical=True):
    return Check(f"{column}_not_null", F.sum(F.col(column).isNull().cast("long")), "<=", 0, critical)


def unique(column, critical=True):
    """Duplicated non-null values."""
    return Check(f"{column}_unique", F.count(column) - F.countDistinct(column), "<=", 0, critical)


def max_null_ratio(column, max_ratio, critical=True):
    return Check(f"{column}_null_ratio", F.avg(F.col(column).isNull().cast("double")), "<=", max_ratio, critical)


def max_ratio(name, numerator, denominator, max_ratio, critical=True):
    return Check(name, numerator / denominator if denominator else 0.0, "<=", max_ratio, critical)


def min_value(name, value, minimum, critical=True):
    return Check(name, value, ">=", minimum, critical)


def no_negatives(columns, critical=True):
    """Rows x columns with a negative value (counts can't be negative)."""
    negatives = reduce(operator.add, [F.when(F.col(c) < 0, 1).otherwise(0) for c in columns])
    return Check("no_negative_counts", F.sum(negatives), "<=", 0, critical)


# The checks below need a groupBy or a join, so they are computed when the Check is built (one action each).

def no_duplicate_keys(df, keys, critical=True):
    """Granularity keys that appear more than once."""
    duplicated = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
    return Check("no_duplicate_keys", duplicated, "<=", 0, critical)


def group_sums_to(df, keys, column, expected=1.0, tolerance=1e-6, critical=True):
    """Groups whose sum(column) is not expected (+- tolerance), e.g. percentages that must add up to 1."""
    sums = df.groupBy(*keys).agg(F.sum(column).alias("_sum"))
    off = sums.filter(F.abs(F.col("_sum") - F.lit(expected)) > tolerance).count()
    return Check(f"{column}_sums_to_{expected:g}", off, "<=", 0, critical)


def max_rows_per_group(df, keys, maximum, critical=True):
    largest = df.groupBy(*keys).count().agg(F.max("count")).first()[0]
    return Check("max_rows_per_group", largest or 0, "<=", maximum, critical)


def counts_match(name, actual, expected, keys, critical=True):
    """Keys where actual.value != expected.value (both DataFrames: keys + a 'value' column; missing = 0)."""
    joined = actual.withColumnRenamed("value", "_actual").join(
        expected.withColumnRenamed("value", "_expected"), list(keys), "full_outer")
    mismatched = joined.filter(F.coalesce("_actual", F.lit(0)) != F.coalesce("_expected", F.lit(0))).count()
    return Check(name, mismatched, "<=", 0, critical)


# ---------- run ----------

def run_checks(df, checks: List[Check]) -> List[CheckResult]:
    aggregates = [check.value.alias(check.name) for check in checks if isinstance(check.value, Column)]
    computed = df.agg(*aggregates).first().asDict() if aggregates else {}
    results = []
    for check in checks:
        value = computed[check.name] if isinstance(check.value, Column) else check.value
        value = float(value or 0)  # an empty DataFrame aggregates to null
        passed = COMPARISONS[check.comparison](value, check.threshold)
        results.append(CheckResult(check.name, value, check.comparison, check.threshold, check.critical, passed))
    return results


def prefixed(results, prefix):
    """Same results named <prefix>.<check>, to run the same checks on several tables."""
    return [replace(r, name=f"{prefix}.{r.name}") for r in results]


def failed_critical(results):
    return [r for r in results if r.critical and not r.passed]


def log_results(results, job, batch):
    for r in results:
        status = "✔" if r.passed else ("✖" if r.critical else "⚠")
        print(f"{status} Check {r.name}: {r.value:g} {r.comparison} {r.threshold:g}")
    print(json.dumps({
        "log": "quality_checks", "job": job, "batch": batch,
        "failed": [r.name for r in results if not r.passed],
        "results": [r.__dict__ for r in results],
    }))


def enforce(results):
    failed = failed_critical(results)
    if failed:
        details = ", ".join(f"{r.name}={r.value:g} (expected {r.comparison} {r.threshold:g})" for r in failed)
        raise QualityCheckError(f"{len(failed)} critical check(s) failed: {details}")
