from __future__ import annotations

import sys

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DecimalType, DoubleType, FloatType, IntegerType, LongType,
    ShortType, ByteType, StringType, DateType, TimestampType, BooleanType,
)

from reports import build_reports


KEYSPACE = "reports"
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042

REPORT_CLUSTER: dict[str, tuple[list[str], str]] = {
    "report_sales_by_product":  (["total_revenue",  "product_id"],  "total_revenue DESC,  product_id ASC"),
    "report_sales_by_customer": (["total_spent",    "customer_id"], "total_spent DESC,    customer_id ASC"),
    "report_sales_by_time":     (["year", "month"],                 "year ASC,            month ASC"),
    "report_sales_by_store":    (["total_revenue",  "store_id"],    "total_revenue DESC,  store_id ASC"),
    "report_sales_by_supplier": (["total_revenue",  "supplier_id"], "total_revenue DESC,  supplier_id ASC"),
    "report_product_quality":   (["product_rating", "product_id"],  "product_rating DESC, product_id ASC"),
}


def spark_to_cql(dt) -> str:
    if isinstance(dt, DecimalType):                        return "decimal"
    if isinstance(dt, (DoubleType, FloatType)):            return "double"
    if isinstance(dt, LongType):                           return "bigint"
    if isinstance(dt, (IntegerType, ShortType, ByteType)): return "int"
    if isinstance(dt, BooleanType):                        return "boolean"
    if isinstance(dt, DateType):                           return "date"
    if isinstance(dt, TimestampType):                      return "timestamp"
    if isinstance(dt, StringType):                         return "text"
    return "text"


def cql_ddl(table: str, df) -> str:
    cluster_cols, cluster_order = REPORT_CLUSTER[table]

    col_defs = (
        "partition_key int, "
        + ", ".join(f"{f.name} {spark_to_cql(f.dataType)}" for f in df.schema.fields)
    )
    pk = f"(partition_key), {', '.join(cluster_cols)}"

    return (
        f"CREATE TABLE {KEYSPACE}.{table} "
        f"({col_defs}, PRIMARY KEY ({pk})) "
        f"WITH CLUSTERING ORDER BY ({cluster_order})"
    )


def setup_keyspace_and_tables(reports: dict):
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()
    try:
        session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")
        session.execute(
            f"CREATE KEYSPACE {KEYSPACE} WITH replication = "
            "{'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        for name, df in reports.items():
            ddl = cql_ddl(name, df)
            print(f"  CQL: {ddl}")
            session.execute(ddl)
    finally:
        cluster.shutdown()


def write_cassandra(df, table: str):
    cluster_cols, _ = REPORT_CLUSTER[table]
    df = df.na.drop(subset=cluster_cols)
    df = df.withColumn("partition_key", F.lit(1))
    (df.write
       .format("org.apache.spark.sql.cassandra")
       .mode("append")
       .options(keyspace=KEYSPACE, table=table)
       .save())
    print(f"  ✓ wrote {table}  ({df.count()} rows)")


def main():
    spark = (SparkSession.builder
             .appName("lab2-etl-cassandra-reports")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("→ building reports from snowflake")
    reports = build_reports(spark)

    print("→ creating cassandra keyspace + tables")
    try:
        setup_keyspace_and_tables(reports)
    except Exception as e:
        print(f"  ✗ Cassandra setup failed: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    print("→ writing reports to cassandra")
    for name, df in reports.items():
        write_cassandra(df, name)

    print("\n✓ all 6 reports written to Cassandra")
    spark.stop()


if __name__ == "__main__":
    main()
