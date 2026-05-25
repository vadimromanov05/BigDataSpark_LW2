from __future__ import annotations

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType


PG_URL = "jdbc:postgresql://postgres:5432/bigdata"
PG_PROPS = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver",
}


def write_pg(df, table):
    (df.write
       .mode("overwrite")
       .option("truncate", "false")
       .jdbc(PG_URL, table, properties=PG_PROPS))
    print(f"  ✓ wrote {table}  ({df.count()} rows)")


def collect_distinct(df, col_expr):
    """Return a sorted Python list of non-null, non-empty distinct string values."""
    return sorted({
        row[0] for row in df.select(col_expr.alias("_v"))
                             .filter(F.col("_v").isNotNull() & (F.col("_v") != ""))
                             .distinct().collect()
    })


def build_sub_dim(spark, raw, source_col, id_col, name_col):
    """
    Build a small lookup dimension entirely on the driver.

    Collects distinct values, sorts them, assigns sequential integer IDs.
    Avoids Window.orderBy() without partitionBy() which would shuffle all
    data to a single partition (harmless here, but noisy in the logs).
    """
    names = collect_distinct(raw, F.trim(F.col(source_col)))
    schema = StructType([
        StructField(id_col,   IntegerType(), False),
        StructField(name_col, StringType(),  True),
    ])
    return spark.createDataFrame(
        [(i + 1, name) for i, name in enumerate(names)],
        schema=schema,
    )


def add_surrogate_id(spark, df, id_col):
    """
    Add a sequential integer surrogate key to a small DataFrame.

    Collects to driver, enumerates in Python, re-creates as DataFrame.
    Safe for sub-dimension tables (hundreds of rows at most).
    """
    rows   = df.collect()
    fields = [StructField(id_col, IntegerType(), False)] + list(df.schema.fields)
    schema = StructType(fields)
    data   = [(i + 1, *row) for i, row in enumerate(rows)]
    return spark.createDataFrame(data, schema=schema)


def main():
    spark = (SparkSession.builder
             .appName("lab2-etl-star-schema")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("→ reading raw_data from postgres")
    raw = spark.read.jdbc(PG_URL, "raw_data", properties=PG_PROPS)
    raw.cache()
    print(f"  raw_data rows: {raw.count()}")

    print("→ building leaf sub-dimensions")

    all_countries = collect_distinct(
        raw.select(F.trim("customer_country").alias("c"))
           .union(raw.select(F.trim("seller_country")))
           .union(raw.select(F.trim("store_country")))
           .union(raw.select(F.trim("supplier_country"))),
        F.col("c"),
    )
    country_schema = StructType([
        StructField("country_id",   IntegerType(), False),
        StructField("country_name", StringType(),  True),
    ])
    country_df = spark.createDataFrame(
        [(i + 1, name) for i, name in enumerate(all_countries)],
        schema=country_schema,
    )

    dim_brand        = build_sub_dim(spark, raw, "product_brand",      "brand_id",        "brand_name")
    dim_category     = build_sub_dim(spark, raw, "product_category",   "category_id",     "category_name")
    dim_pet_category = build_sub_dim(spark, raw, "pet_category",       "pet_category_id", "pet_category_name")
    dim_pet_type     = build_sub_dim(spark, raw, "customer_pet_type",  "pet_type_id",     "pet_type_name")
    dim_pet_breed    = build_sub_dim(spark, raw, "customer_pet_breed", "pet_breed_id",    "pet_breed_name")
    dim_material     = build_sub_dim(spark, raw, "product_material",   "material_id",     "material_name")

    write_pg(country_df,       "dim_country")
    write_pg(dim_brand,        "dim_brand")
    write_pg(dim_category,     "dim_category")
    write_pg(dim_pet_category, "dim_pet_category")
    write_pg(dim_pet_type,     "dim_pet_type")
    write_pg(dim_pet_breed,    "dim_pet_breed")
    write_pg(dim_material,     "dim_material")

    country_df.cache()
    dim_brand.cache()
    dim_category.cache()
    dim_pet_category.cache()
    dim_pet_type.cache()
    dim_pet_breed.cache()
    dim_material.cache()

    print("→ building dim_supplier")

    supplier_src = (raw
        .filter(F.col("supplier_name").isNotNull() & (F.trim("supplier_name") != ""))
        .dropDuplicates(["supplier_name"])
        .select(
            F.trim("supplier_name").alias("supplier_name"),
            F.trim("supplier_contact").alias("supplier_contact"),
            F.trim("supplier_email").alias("supplier_email"),
            F.trim("supplier_phone").alias("supplier_phone"),
            F.trim("supplier_address").alias("supplier_address"),
            F.trim("supplier_city").alias("supplier_city"),
            F.trim("supplier_country").alias("supplier_country"),
        )
        .orderBy("supplier_name")
    )

    supplier_joined = (supplier_src
        .join(country_df, supplier_src.supplier_country == country_df.country_name, "left")
        .select(
            "supplier_name", "supplier_contact", "supplier_email",
            "supplier_phone", "supplier_address", "supplier_city", "country_id",
        )
    )

    dim_supplier = add_surrogate_id(spark, supplier_joined, "supplier_id")
    write_pg(dim_supplier, "dim_supplier")
    dim_supplier.cache()

    print("→ building dim_customer")

    cust_src = (raw
        .filter(F.col("sale_customer_id").isNotNull())
        .dropDuplicates(["sale_customer_id"])
        .select(
            F.col("sale_customer_id").cast("int").alias("customer_id"),
            F.trim("customer_first_name").alias("customer_first_name"),
            F.trim("customer_last_name").alias("customer_last_name"),
            F.col("customer_age").cast("int").alias("customer_age"),
            F.trim("customer_email").alias("customer_email"),
            F.trim("customer_postal_code").alias("customer_postal_code"),
            F.trim("customer_country").alias("customer_country"),
            F.trim("customer_pet_type").alias("customer_pet_type"),
            F.trim("customer_pet_breed").alias("customer_pet_breed"),
            F.trim("customer_pet_name").alias("customer_pet_name"),
        )
    )

    dim_customer = (cust_src
        .join(country_df,    cust_src.customer_country   == country_df.country_name,      "left")
        .join(dim_pet_type,  cust_src.customer_pet_type  == dim_pet_type.pet_type_name,   "left")
        .join(dim_pet_breed, cust_src.customer_pet_breed == dim_pet_breed.pet_breed_name, "left")
        .select(
            "customer_id", "customer_first_name", "customer_last_name",
            "customer_age", "customer_email", "customer_postal_code",
            "country_id", "pet_type_id", "pet_breed_id", "customer_pet_name",
        )
    )
    write_pg(dim_customer, "dim_customer")

    print("→ building dim_seller")

    sell_src = (raw
        .filter(F.col("sale_seller_id").isNotNull())
        .dropDuplicates(["sale_seller_id"])
        .select(
            F.col("sale_seller_id").cast("int").alias("seller_id"),
            F.trim("seller_first_name").alias("seller_first_name"),
            F.trim("seller_last_name").alias("seller_last_name"),
            F.trim("seller_email").alias("seller_email"),
            F.trim("seller_postal_code").alias("seller_postal_code"),
            F.trim("seller_country").alias("seller_country"),
        )
    )

    dim_seller = (sell_src
        .join(country_df, sell_src.seller_country == country_df.country_name, "left")
        .select(
            "seller_id", "seller_first_name", "seller_last_name",
            "seller_email", "seller_postal_code", "country_id",
        )
    )
    write_pg(dim_seller, "dim_seller")

    print("→ building dim_product")

    prod_src = (raw
        .filter(F.col("sale_product_id").isNotNull())
        .dropDuplicates(["sale_product_id"])
        .select(
            F.col("sale_product_id").cast("int").alias("product_id"),
            F.trim("product_name").alias("product_name"),
            F.trim("product_category").alias("product_category"),
            F.trim("pet_category").alias("pet_category"),
            F.trim("product_brand").alias("product_brand"),
            F.trim("product_material").alias("product_material"),
            F.trim("supplier_name").alias("supplier_name"),
            F.col("product_price").cast("decimal(12,2)").alias("product_price"),
            F.col("product_quantity").cast("int").alias("product_quantity"),
            F.col("product_weight").cast("decimal(10,2)").alias("product_weight"),
            F.trim("product_color").alias("product_color"),
            F.trim("product_size").alias("product_size"),
            F.col("product_description").alias("product_description"),
            F.col("product_rating").cast("decimal(4,2)").alias("product_rating"),
            F.col("product_reviews").cast("int").alias("product_reviews"),
            F.to_date(F.col("product_release_date"), "M/d/yyyy").alias("product_release_date"),
            F.to_date(F.col("product_expiry_date"),  "M/d/yyyy").alias("product_expiry_date"),
        )
    )

    dim_product = (prod_src
        .join(dim_category,     prod_src.product_category == dim_category.category_name,         "left")
        .join(dim_pet_category, prod_src.pet_category     == dim_pet_category.pet_category_name, "left")
        .join(dim_brand,        prod_src.product_brand    == dim_brand.brand_name,               "left")
        .join(dim_material,     prod_src.product_material == dim_material.material_name,         "left")
        .join(dim_supplier,     prod_src.supplier_name    == dim_supplier.supplier_name,         "left")
        .select(
            "product_id", "product_name",
            "category_id", "pet_category_id", "brand_id", "material_id", "supplier_id",
            "product_price", "product_quantity", "product_weight",
            "product_color", "product_size", "product_description",
            "product_rating", "product_reviews",
            "product_release_date", "product_expiry_date",
        )
    )
    write_pg(dim_product, "dim_product")

    print("→ building dim_store")

    store_src = (raw
        .filter(F.col("store_name").isNotNull() & (F.trim("store_name") != ""))
        .dropDuplicates(["store_name"])
        .select(
            F.trim("store_name").alias("store_name"),
            F.trim("store_location").alias("store_location"),
            F.trim("store_city").alias("store_city"),
            F.trim("store_state").alias("store_state"),
            F.trim("store_country").alias("store_country"),
            F.trim("store_phone").alias("store_phone"),
            F.trim("store_email").alias("store_email"),
        )
        .orderBy("store_name")   # deterministic ordering before collect
    )

    store_joined = (store_src
        .join(country_df, store_src.store_country == country_df.country_name, "left")
        .select(
            "store_name", "store_location", "store_city",
            "store_state", "country_id", "store_phone", "store_email",
        )
    )

    dim_store = add_surrogate_id(spark, store_joined, "store_id")
    write_pg(dim_store, "dim_store")
    
    print("→ building fact_sales")

    store_lookup = dim_store.select("store_id", "store_name")

    fact_src = (raw
        .select(
            F.col("sale_customer_id").cast("int").alias("customer_id"),
            F.col("sale_seller_id").cast("int").alias("seller_id"),
            F.col("sale_product_id").cast("int").alias("product_id"),
            F.trim("store_name").alias("store_name"),
            F.to_date(F.col("sale_date"), "M/d/yyyy").alias("sale_date"),
            F.col("sale_quantity").cast("int").alias("sale_quantity"),
            F.col("sale_total_price").cast("decimal(14,2)").alias("sale_total_price"),
        )
    )

    fact_sales = (fact_src
        .join(store_lookup, fact_src.store_name == store_lookup.store_name, "left")
        .withColumn("sale_id", F.monotonically_increasing_id() + 1)
        .select(
            "sale_id", "customer_id", "seller_id", "product_id", "store_id",
            "sale_date", "sale_quantity", "sale_total_price",
        )
    )
    write_pg(fact_sales, "fact_sales")

    print("\n=== check ===")
    n_fact   = fact_sales.count()
    n_null_c = fact_sales.filter(F.col("customer_id").isNull()).count()
    n_null_s = fact_sales.filter(F.col("seller_id").isNull()).count()
    n_null_p = fact_sales.filter(F.col("product_id").isNull()).count()
    n_null_t = fact_sales.filter(F.col("store_id").isNull()).count()
    print(f"  fact_sales rows : {n_fact}")
    print(f"  null FKs        : customer={n_null_c} seller={n_null_s} "
          f"product={n_null_p} store={n_null_t}")

    spark.stop()


if __name__ == "__main__":
    main()
