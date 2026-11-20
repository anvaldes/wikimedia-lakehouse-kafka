import argparse
from datetime import datetime, timezone

MODES = ["stream", "backfill", "continuous"]


def parse_args():
    parser = argparse.ArgumentParser()
    for name in ("year", "month", "day", "hour"):
        parser.add_argument(f"--{name}", required=True, type=int)
    parser.add_argument("--mode", choices=MODES, default="stream")
    return parser.parse_args()


def run_hour(args):
    """The UTC hour this run belongs to. Raises ValueError if the date is invalid."""
    return datetime(args.year, args.month, args.day, args.hour, tzinfo=timezone.utc)
