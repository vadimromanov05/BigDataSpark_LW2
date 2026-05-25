from __future__ import annotations

from decimal import Decimal
from datetime import date, datetime

import redis
from pyspark.sql import SparkSession

from reports import build_reports, REPORT_KEYS


VALKEY_HOST = "valkey"
VALKEY_PORT = 6379


def coerce(v):
    if v is None:
        return ""
    if isinstance(v, Decimal):
        return str(float(v))
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def df_to_dicts(df):
    df = df.na.fill("").na.fill(0)
    return [{k: coerce(v) for k, v in row.asDict().items()}
            for row in df.collect()]


def write_valkey(r: redis.Redis, report_name: str, df):
    pks  = REPORT_KEYS[report_name]
    rows = df_to_dicts(df)

    index_key = f"{report_name}:_keys"
    old_keys = r.smembers(index_key)
    pipe = r.pipeline()
    if old_keys:
        pipe.delete(*old_keys)
    pipe.delete(index_key)
    pipe.execute()

    pipe = r.pipeline()
    for row in rows:
        suffix = ":".join(str(row.get(k, "")) for k in pks)
        key = f"{report_name}:{suffix}"
        flat = {k: v for k, v in row.items()}
        pipe.hset(key, mapping=flat)
        pipe.sadd(index_key, key)
    pipe.execute()
    print(f"  ✓ wrote {report_name}  ({len(rows)} keys)")


def main():
    spark = (SparkSession.builder
             .appName("lab2-etl-valkey-reports")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("→ building reports from snowflake")
    reports = build_reports(spark)

    print("→ writing reports to valkey")
    r = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, decode_responses=True)
    r.ping()

    for name, df in reports.items():
        write_valkey(r, name, df)

    print("\n✓ all 6 reports written to Valkey")
    spark.stop()


if __name__ == "__main__":
    main()
