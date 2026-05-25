from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver",
}

REPORT_KEYS = {
    "report_sales_by_product":  ["product_id"],
    "report_sales_by_customer": ["customer_id"],
    "report_sales_by_time":     ["year", "month"],
    "report_sales_by_store":    ["store_id"],
    "report_sales_by_supplier": ["supplier_id"],
    "report_product_quality":   ["product_id"],
}


def read_snowflake(spark: SparkSession) -> dict[str, DataFrame]:
    """Load every dim + fact table from postgres into a dict."""
    names = [
        "fact_sales", "dim_customer", "dim_seller", "dim_product",
        "dim_store", "dim_supplier", "dim_country", "dim_category",
        "dim_brand", "dim_material", "dim_pet_type", "dim_pet_breed",
        "dim_pet_category",
    ]
    return {n: spark.read.jdbc(PG_URL, n, properties=PG_PROPS).cache()
            for n in names}


def build_reports(spark: SparkSession) -> dict[str, DataFrame]:
    """Build all 6 report DataFrames. Returns dict keyed by report name."""
    t = read_snowflake(spark)
    fact, customer, product, store     = t["fact_sales"], t["dim_customer"], t["dim_product"], t["dim_store"]
    supplier, country, category, brand = t["dim_supplier"], t["dim_country"], t["dim_category"], t["dim_brand"]

    out: dict[str, DataFrame] = {}

    fp = fact.join(product, "product_id")
    per_product = (fp.groupBy(
            "product_id", "product_name", "category_id", "brand_id",
            "product_rating", "product_reviews")
        .agg(
            F.sum("sale_quantity").alias("total_quantity"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("*").alias("sales_count"),
        ))
    cat_rev = (fp.join(category, "category_id", "left")
        .groupBy("category_id", "category_name")
        .agg(F.sum("sale_total_price").alias("category_revenue")))

    out["report_sales_by_product"] = (per_product
        .join(category, "category_id", "left")
        .join(brand,    "brand_id",    "left")
        .join(cat_rev.select("category_id", "category_revenue"), "category_id", "left")
        .select(
            "product_id", "product_name", "category_name", "brand_name",
            "total_quantity", "total_revenue", "sales_count",
            "product_rating", "product_reviews", "category_revenue")
        .orderBy(F.col("total_revenue").desc()))

    cust_sales = (fact.join(customer, "customer_id")
        .groupBy(
            "customer_id", "customer_first_name", "customer_last_name",
            "customer_email", "country_id")
        .agg(
            F.sum("sale_total_price").alias("total_spent"),
            F.count("*").alias("orders_count"),
            F.avg("sale_total_price").alias("avg_ticket")))

    out["report_sales_by_customer"] = (cust_sales
        .join(country, "country_id", "left")
        .select(
            "customer_id",
            F.concat_ws(" ", "customer_first_name", "customer_last_name").alias("customer_name"),
            "customer_email", "country_name",
            "total_spent", "orders_count", "avg_ticket")
        .orderBy(F.col("total_spent").desc()))

    out["report_sales_by_time"] = (fact
        .withColumn("year",  F.year("sale_date"))
        .withColumn("month", F.month("sale_date"))
        .filter(F.col("year").isNotNull() & F.col("month").isNotNull())
        .groupBy("year", "month")
        .agg(
            F.sum("sale_total_price").alias("revenue"),
            F.sum("sale_quantity").alias("units_sold"),
            F.count("*").alias("orders_count"),
            F.avg("sale_total_price").alias("avg_order"))
        .orderBy("year", "month"))

    store_sales = (fact.join(store, "store_id")
        .groupBy("store_id", "store_name", "store_city", "store_state", "country_id")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("*").alias("orders_count"),
            F.avg("sale_total_price").alias("avg_ticket")))

    out["report_sales_by_store"] = (store_sales
        .join(country, "country_id", "left")
        .select(
            "store_id", "store_name", "store_city", "store_state",
            "country_name", "total_revenue", "orders_count", "avg_ticket")
        .orderBy(F.col("total_revenue").desc()))

    fps = fact.join(product, "product_id").join(supplier, "supplier_id")
    sup_sales = (fps
        .groupBy("supplier_id", "supplier_name", "supplier_city", "country_id")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("*").alias("orders_count"),
            F.avg("product_price").alias("avg_product_price")))

    out["report_sales_by_supplier"] = (sup_sales
        .join(country, "country_id", "left")
        .select(
            "supplier_id", "supplier_name", "supplier_city",
            "country_name", "total_revenue", "orders_count", "avg_product_price")
        .orderBy(F.col("total_revenue").desc()))

    sales_per_product = (fact.groupBy("product_id")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.sum("sale_quantity").alias("total_quantity")))

    out["report_product_quality"] = (product
        .join(sales_per_product, "product_id", "left")
        .join(category, "category_id", "left")
        .select(
            "product_id", "product_name", "category_name",
            "product_rating", "product_reviews",
            F.coalesce("total_revenue",  F.lit(0)).alias("total_revenue"),
            F.coalesce("total_quantity", F.lit(0)).alias("total_quantity"))
        .orderBy(F.col("product_rating").desc_nulls_last(),
                 F.col("product_reviews").desc_nulls_last()))

    return out
