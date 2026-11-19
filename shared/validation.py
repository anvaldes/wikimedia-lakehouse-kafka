import json
from datetime import datetime

REQUIRED_FIELDS = [("meta", "id"), ("meta", "dt"), ("wiki",), ("type",)]


def parse_dt(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_field(event, path):
    value = event
    for key in path:
        value = value.get(key) if isinstance(value, dict) else None
    return value


def parse_and_validate(raw):
    """Returns (event, error_reason). error_reason is None when the event is valid."""
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        return None, "invalid_json"
    if not isinstance(event, dict):
        return None, "invalid_json"
    for path in REQUIRED_FIELDS:
        if get_field(event, path) in (None, ""):
            return event, f"missing_{'.'.join(path)}"
    try:
        parse_dt(event["meta"]["dt"])
    except (ValueError, TypeError, AttributeError):
        return event, "invalid_meta.dt"
    return event, None
