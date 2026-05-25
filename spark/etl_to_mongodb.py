from __future__ import annotations

from pyspark.sql import SparkSession

from reports import build_reports


MONGO_URI = "mongodb://mongo:27017"
DATABASE  = "reports"


def write_mongo(df, collection):
    (df.write
       .format("mongodb")
       .mode("overwrite")
       .option("connection.uri", MONGO_URI)
       .option("database", DATABASE)
       .option("collection", collection)
       .save())
    print(f"  ✓ wrote {DATABASE}.{collection}  ({df.count()} docs)")


def main():
    spark = (SparkSession.builder
             .appName("lab2-etl-mongodb-reports")
             .config("spark.mongodb.read.connection.uri",  MONGO_URI)
             .config("spark.mongodb.write.connection.uri", MONGO_URI)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("→ building reports from snowflake")
    reports = build_reports(spark)

    print("→ writing reports to mongodb")
    for name, df in reports.items():
        write_mongo(df, name)

    print("\n✓ all 6 reports written to MongoDB")
    spark.stop()


if __name__ == "__main__":
    main()
