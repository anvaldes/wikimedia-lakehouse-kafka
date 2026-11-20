import re
from importlib.resources import files

# A statement ends with ';' at the end of a line, so ';' inside comments or strings is safe.
STATEMENT_END = re.compile(r";\s*$", re.MULTILINE)


def read_migration(name):
    return files("processing.migrations").joinpath(name).read_text()


def split_statements(sql):
    return [statement.strip() for statement in STATEMENT_END.split(sql) if statement.strip()]


def run_migrations(spark, names):
    """Migrations are idempotent (IF NOT EXISTS), so they run at the start of every job."""
    for name in names:
        for statement in split_statements(read_migration(name)):
            spark.sql(statement)
        print(f"✔ Migration applied: {name}")
