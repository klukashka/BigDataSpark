import os

from pyspark.sql import SparkSession, functions as F


def jdbc_url_postgres() -> str:
    """
    Build JDBC URL for PostgreSQL inside docker-compose network.

    :return: JDBC URL for PostgreSQL.
    """
    db = os.environ.get("POSTGRES_DB", "bigdata_spark")
    return f"jdbc:postgresql://postgres:5432/{db}"


def main() -> None:
    """
    Build star schema in PostgreSQL using Spark.

    Source: `stage.mock_data` in PostgreSQL.
    Target: `dw.*` (dimensions + `dw.fact_sales`) in PostgreSQL.
    """
    pg_user = os.environ.get("POSTGRES_USER", "bigdata")
    pg_pass = os.environ.get("POSTGRES_PASSWORD", "bigdata")

    spark = (
        SparkSession.builder.appName("build_star_postgres")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    src = (
        spark.read.format("jdbc")
        .option("url", jdbc_url_postgres())
        .option("dbtable", "stage.mock_data")
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Dimensions
    dim_date = (
        src.select(F.col("sale_date").alias("full_date"))
        .where(F.col("full_date").isNotNull())
        .distinct()
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .select("date_key", "full_date", "year", "quarter", "month", "day")
    )

    dim_customer = (
        src.select(
            F.col("sale_customer_id").alias("customer_id"),
            F.col("customer_first_name").alias("first_name"),
            F.col("customer_last_name").alias("last_name"),
            F.col("customer_age").alias("age"),
            F.col("customer_email").alias("email"),
            F.col("customer_country").alias("country"),
            F.col("customer_postal_code").alias("postal_code"),
            F.col("customer_pet_type").alias("pet_type"),
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("pet_breed"),
        )
        .where(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )

    dim_seller = (
        src.select(
            F.col("sale_seller_id").alias("seller_id"),
            F.col("seller_first_name").alias("first_name"),
            F.col("seller_last_name").alias("last_name"),
            F.col("seller_email").alias("email"),
            F.col("seller_country").alias("country"),
            F.col("seller_postal_code").alias("postal_code"),
        )
        .where(F.col("seller_id").isNotNull())
        .dropDuplicates(["seller_id"])
    )

    dim_product = (
        src.select(
            F.col("sale_product_id").alias("product_id"),
            F.col("product_name").alias("name"),
            F.col("product_category").alias("category"),
            F.col("pet_category").alias("pet_category"),
            F.col("product_price").alias("price"),
            F.col("product_quantity").alias("quantity_in_source"),
            F.col("product_weight").alias("weight"),
            F.col("product_color").alias("color"),
            F.col("product_size").alias("size"),
            F.col("product_brand").alias("brand"),
            F.col("product_material").alias("material"),
            F.col("product_description").alias("description"),
            F.col("product_rating").alias("rating"),
            F.col("product_reviews").alias("reviews"),
            F.col("product_release_date").alias("release_date"),
            F.col("product_expiry_date").alias("expiry_date"),
        )
        .where(F.col("product_id").isNotNull())
        .dropDuplicates(["product_id"])
    )

    # Store/supplier get surrogate keys based on distinct attribute sets
    dim_store = (
        src.select(
            F.col("store_name").alias("name"),
            F.col("store_location").alias("location"),
            F.col("store_city").alias("city"),
            F.col("store_state").alias("state"),
            F.col("store_country").alias("country"),
            F.col("store_phone").alias("phone"),
            F.col("store_email").alias("email"),
        )
        .where(F.col("name").isNotNull())
        .distinct()
        .withColumn(
            "store_key",
            F.xxhash64("name", "location", "city", "state", "country", "phone", "email"),
        )
        .select("store_key", "name", "location", "city", "state", "country", "phone", "email")
    )

    dim_supplier = (
        src.select(
            F.col("supplier_name").alias("name"),
            F.col("supplier_contact").alias("contact"),
            F.col("supplier_email").alias("email"),
            F.col("supplier_phone").alias("phone"),
            F.col("supplier_address").alias("address"),
            F.col("supplier_city").alias("city"),
            F.col("supplier_country").alias("country"),
        )
        .where(F.col("name").isNotNull())
        .distinct()
        .withColumn(
            "supplier_key",
            F.xxhash64("name", "contact", "email", "phone", "address", "city", "country"),
        )
        .select("supplier_key", "name", "contact", "email", "phone", "address", "city", "country")
    )

    fact = (
        src.select(
            F.col("id").alias("sale_id"),
            F.col("sale_date"),
            F.col("sale_customer_id").alias("customer_id"),
            F.col("sale_seller_id").alias("seller_id"),
            F.col("sale_product_id").alias("product_id"),
            F.col("sale_quantity").alias("sale_quantity"),
            F.col("sale_total_price").alias("total_price"),
            F.col("store_name"),
            F.col("store_location"),
            F.col("store_city"),
            F.col("store_state"),
            F.col("store_country"),
            F.col("store_phone"),
            F.col("store_email"),
            F.col("supplier_name"),
            F.col("supplier_contact"),
            F.col("supplier_email"),
            F.col("supplier_phone"),
            F.col("supplier_address"),
            F.col("supplier_city"),
            F.col("supplier_country"),
        )
        .where(F.col("sale_id").isNotNull() & F.col("sale_date").isNotNull())
        .withColumn("date_key", F.date_format("sale_date", "yyyyMMdd").cast("int"))
        .withColumn(
            "store_key",
            F.xxhash64(
                "store_name",
                "store_location",
                "store_city",
                "store_state",
                "store_country",
                "store_phone",
                "store_email",
            ),
        )
        .withColumn(
            "supplier_key",
            F.xxhash64(
                "supplier_name",
                "supplier_contact",
                "supplier_email",
                "supplier_phone",
                "supplier_address",
                "supplier_city",
                "supplier_country",
            ),
        )
        .select(
            "sale_id",
            "date_key",
            "customer_id",
            "seller_id",
            "product_id",
            "store_key",
            "supplier_key",
            "sale_quantity",
            "total_price",
        )
    )

    # Write into Postgres (schema dw). We recreate tables each run.
    for name, df in [
        ("dw.dim_date", dim_date),
        ("dw.dim_customer", dim_customer),
        ("dw.dim_seller", dim_seller),
        ("dw.dim_product", dim_product),
        ("dw.dim_store", dim_store),
        ("dw.dim_supplier", dim_supplier),
        ("dw.fact_sales", fact),
    ]:
        (
            df.write.format("jdbc")
            .mode("overwrite")
            .option("url", jdbc_url_postgres())
            .option("dbtable", name)
            .option("user", pg_user)
            .option("password", pg_pass)
            .option("driver", "org.postgresql.Driver")
            .save()
        )

    spark.stop()


if __name__ == "__main__":
    main()

