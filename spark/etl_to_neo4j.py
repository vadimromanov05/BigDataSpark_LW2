from __future__ import annotations

from decimal import Decimal
from datetime import date, datetime

from neo4j import GraphDatabase
from pyspark.sql import SparkSession

from reports import build_reports, REPORT_KEYS


NEO4J_URI  = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "test1234"


def coerce(v):
    """Neo4j Bolt cannot serialize Decimal, and we want clean dates as ISO."""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def df_to_dicts(df):
    """Materialize a DataFrame to a list of native-python dicts."""
    df = df.na.fill("").na.fill(0)
    return [{k: coerce(v) for k, v in row.asDict().items()}
            for row in df.collect()]


def label_for(report_name: str) -> str:
    return report_name[0].upper() + report_name[1:]


def write_neo4j(driver, report_name: str, df):
    label = label_for(report_name)
    pks   = REPORT_KEYS[report_name]
    rows  = df_to_dicts(df)

    merge_clause = ", ".join(f"{k}: row.{k}" for k in pks)
    cypher = (
        f"UNWIND $rows AS row "
        f"MERGE (n:{label} {{{merge_clause}}}) "
        f"SET n += row"
    )

    with driver.session() as sess:
        sess.run(f"MATCH (n:{label}) DETACH DELETE n")
        BATCH = 500
        for i in range(0, len(rows), BATCH):
            sess.run(cypher, rows=rows[i:i + BATCH])
    print(f"  ✓ wrote :{label}  ({len(rows)} nodes)")


def main():
    spark = (SparkSession.builder
             .appName("lab2-etl-neo4j-reports")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("→ building reports from snowflake")
    reports = build_reports(spark)

    print("→ writing reports to neo4j")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    try:
        for name, df in reports.items():
            write_neo4j(driver, name, df)
    finally:
        driver.close()

    print("\n✓ all 6 reports written to Neo4j")
    spark.stop()


if __name__ == "__main__":
    main()
