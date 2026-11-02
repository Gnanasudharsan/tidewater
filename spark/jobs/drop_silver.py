"""Chaos helper: delete every silver table plus the quarantine and late-arrival logs.

    spark-submit jobs/drop_silver.py --yes-really

Used by chaos test 05 (rebuild silver from bronze). Bronze is untouched.
"""

import argparse
import sys

from tidewater_spark.config import Settings
from tidewater_spark.session import build_spark
from tidewater_spark.tables import load_contracts

parser = argparse.ArgumentParser()
parser.add_argument("--yes-really", action="store_true")
if not parser.parse_args().yes_really:
    sys.exit("refusing to drop silver without --yes-really")
settings = Settings.from_env()
spark = build_spark("tidewater-drop-silver", settings)
jvm, conf = spark._jvm, spark._jsc.hadoopConfiguration()
paths = [settings.silver_path(t) for t in load_contracts()]
paths += [settings.quarantine_path, settings.late_arrivals_path]
for p in paths:
    path = jvm.org.apache.hadoop.fs.Path(p)
    fs = path.getFileSystem(conf)
    print(f"DROPPED {p}" if fs.delete(path, True) else f"absent  {p}")
