from datetime import datetime, timezone


def get_partition(dt=None):
    dt = dt or datetime.now(timezone.utc)
    return {"year": f"{dt:%Y}", "month": f"{dt:%m}", "day": f"{dt:%d}", "hour": f"{dt:%H}"}


def build_path(prefix, partition):
    p = partition
    return f"{prefix}/year={p['year']}/month={p['month']}/day={p['day']}/hour={p['hour']}"