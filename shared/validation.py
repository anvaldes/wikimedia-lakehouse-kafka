import json
from datetime import datetime

REQUIRED_FIELDS = [("meta", "id"), ("meta", "dt"), ("wiki",), ("type",)]


class NonFiniteNumber(ValueError):
    pass


def parse_dt(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_field(event, path):
    value = event
    for key in path:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def reject_non_finite(value):
    raise NonFiniteNumber(value)


def load_json(raw):
    """Returns (value, error_reason). NaN / Infinity are rejected: they are not valid JSON."""
    try:
        return json.loads(raw, parse_constant=reject_non_finite), None
    except NonFiniteNumber:
        return None, "non_finite_number"
    except json.JSONDecodeError:
        return None, "invalid_json"


def check_required(event):
    for path in REQUIRED_FIELDS:
        value, name = get_field(event, path), ".".join(path)
        if value is None or value == "":
            return f"missing_{name}"
        if not isinstance(value, str):
            return f"invalid_type_{name}"
    return None


def check_dt(event):
    try:
        parse_dt(event["meta"]["dt"])
        return None
    except ValueError:
        return "invalid_meta.dt"


def parse_and_validate(raw):
    """Returns (event, error_reason). error_reason is None when the event is valid."""
    if raw is None or not raw.strip():
        return None, "empty_value"
    event, error = load_json(raw)
    if error:
        return None, error
    if not isinstance(event, dict):
        return None, "not_json_object"
    return event, check_required(event) or check_dt(event)
