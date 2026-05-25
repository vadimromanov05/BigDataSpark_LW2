
from pyspark.sql import SparkSession

from reports import build_reports


CH_URL   = "jdbc:clickhouse://clickhouse:8123/reports"
CH_PROPS = {
    "user": "default",
    "password": "clickhouse",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def write_ch(df, table):
    df = df.na.fill("").na.fill(0)
    (df.write
       .mode("overwrite")
       .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
       .jdbc(CH_URL, table, properties=CH_PROPS))
    print(f"  ✓ wrote {table}  ({df.count()} rows)")


def main():
    spark = (SparkSession.builder
             .appName("lab2-etl-clickhouse-reports")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("→ building reports from snowflake")
    reports = build_reports(spark)

    for name, df in reports.items():
        write_ch(df, name)

    print("\n✓ all 6 reports written to ClickHouse")
    spark.stop()


if __name__ == "__main__":
    main()
