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
from dataclasses import dataclass
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
