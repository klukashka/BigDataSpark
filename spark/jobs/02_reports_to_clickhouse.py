import os

from pyspark.sql import SparkSession, functions as F


def pg_url() -> str:
    """
    Build JDBC URL for PostgreSQL inside docker-compose network.

    :return: JDBC URL for PostgreSQL.
    """
    db = os.environ.get("POSTGRES_DB", "bigdata_spark")
    return f"jdbc:postgresql://postgres:5432/{db}"


def ch_url() -> str:
    """
    Build JDBC URL for ClickHouse inside docker-compose network.

    ClickHouse JDBC typically uses HTTP port (8123).

    :return: JDBC URL for ClickHouse.
    """
    db = os.environ.get("CLICKHOUSE_DB", "analytics")
    return f"jdbc:clickhouse://clickhouse:8123/{db}"


def create_clickhouse_tables(spark: SparkSession) -> None:
    """
    Create 6 report tables in ClickHouse (idempotent).

    Uses JVM JDBC directly to run DDL.

    :param spark: Active Spark session (used to access JVM).
    """
    jvm = spark._sc._gateway.jvm  # type: ignore[attr-defined]
    conn = jvm.java.sql.DriverManager.getConnection(ch_url())
    stmt = conn.createStatement()
    try:
        # 1) Products mart
        stmt.execute(
            """
            CREATE TABLE IF NOT EXISTS mart_sales_by_products (
              product_id Int32,
              product_name String,
              product_category String,
              revenue Float64,
              sales_qty Int64,
              orders_cnt Int64,
              avg_rating Float64,
              reviews_cnt Int64
            ) ENGINE = MergeTree
            ORDER BY (revenue, sales_qty)
            """
        )

        # 2) Customers mart
        stmt.execute(
            """
            CREATE TABLE IF NOT EXISTS mart_sales_by_customers (
              customer_id Int32,
              customer_country String,
              full_name String,
              total_spent Float64,
              orders_cnt Int64,
              avg_check Float64
            ) ENGINE = MergeTree
            ORDER BY (total_spent, orders_cnt)
            """
        )

        # 3) Time mart (month level)
        stmt.execute(
            """
            CREATE TABLE IF NOT EXISTS mart_sales_by_time (
              year Int32,
              month Int32,
              revenue Float64,
              orders_cnt Int64,
              avg_order_size Float64
            ) ENGINE = MergeTree
            ORDER BY (year, month)
            """
        )

        # 4) Stores mart
        stmt.execute(
            """
            CREATE TABLE IF NOT EXISTS mart_sales_by_stores (
              store_key Int64,
              store_name String,
              store_city String,
              store_country String,
              revenue Float64,
              orders_cnt Int64,
              avg_check Float64
            ) ENGINE = MergeTree
            ORDER BY (revenue, orders_cnt)
            """
        )

        # 5) Suppliers mart
        stmt.execute(
            """
            CREATE TABLE IF NOT EXISTS mart_sales_by_suppliers (
              supplier_key Int64,
              supplier_name String,
              supplier_country String,
              revenue Float64,
              avg_price Float64,
              orders_cnt Int64
            ) ENGINE = MergeTree
            ORDER BY (revenue, orders_cnt)
            """
        )

        # 6) Product quality mart
        stmt.execute(
            """
            CREATE TABLE IF NOT EXISTS mart_product_quality (
              product_id Int32,
              product_name String,
              product_category String,
              rating Float64,
              reviews_cnt Int64,
              sales_qty Int64,
              revenue Float64
            ) ENGINE = MergeTree
            ORDER BY (rating, reviews_cnt)
            """
        )
    finally:
        stmt.close()
        conn.close()


def write_to_clickhouse(df, table: str, mode: str = "overwrite") -> None:
    """
    Write dataframe to ClickHouse via JDBC.

    :param df: Spark DataFrame to write.
    :param table: Target ClickHouse table name.
    :param mode: Spark write mode (default: overwrite).
    """
    (
        df.write.format("jdbc")
        .mode(mode)
        .option("url", ch_url())
        .option("dbtable", table)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .save()
    )


def main() -> None:
    """
    Build 6 report marts and store them in ClickHouse.

    Source: star schema in PostgreSQL (`dw.*`).
    Target: ClickHouse tables `mart_*` in database `analytics` (by default).
    """
    pg_user = os.environ.get("POSTGRES_USER", "bigdata")
    pg_pass = os.environ.get("POSTGRES_PASSWORD", "bigdata")

    spark = (
        SparkSession.builder.appName("reports_to_clickhouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    create_clickhouse_tables(spark)

    fact = (
        spark.read.format("jdbc")
        .option("url", pg_url())
        .option("dbtable", "dw.fact_sales")
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    dim_product = (
        spark.read.format("jdbc")
        .option("url", pg_url())
        .option("dbtable", "dw.dim_product")
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    dim_customer = (
        spark.read.format("jdbc")
        .option("url", pg_url())
        .option("dbtable", "dw.dim_customer")
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    dim_date = (
        spark.read.format("jdbc")
        .option("url", pg_url())
        .option("dbtable", "dw.dim_date")
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    dim_store = (
        spark.read.format("jdbc")
        .option("url", pg_url())
        .option("dbtable", "dw.dim_store")
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    dim_supplier = (
        spark.read.format("jdbc")
        .option("url", pg_url())
        .option("dbtable", "dw.dim_supplier")
        .option("user", pg_user)
        .option("password", pg_pass)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # 1) Sales by products
    by_products = (
        fact.join(dim_product, on="product_id", how="left")
        .groupBy(
            "product_id",
            F.col("name").alias("product_name"),
            F.col("category").alias("product_category"),
        )
        .agg(
            F.sum(F.col("total_price").cast("double")).alias("revenue"),
            F.sum(F.col("sale_quantity").cast("long")).alias("sales_qty"),
            F.countDistinct("sale_id").alias("orders_cnt"),
            F.avg(F.col("rating").cast("double")).alias("avg_rating"),
            F.max(F.col("reviews").cast("long")).alias("reviews_cnt"),
        )
    )
    write_to_clickhouse(
        by_products.selectExpr(
            "product_id",
            "product_name",
            "product_category",
            "revenue",
            "sales_qty",
            "orders_cnt",
            "avg_rating",
            "reviews_cnt",
        ),
        "mart_sales_by_products",
    )

    # 2) Sales by customers
    by_customers = (
        fact.join(dim_customer, on="customer_id", how="left")
        .groupBy(
            "customer_id",
            F.col("country").alias("customer_country"),
            F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        )
        .agg(
            F.sum(F.col("total_price").cast("double")).alias("total_spent"),
            F.countDistinct("sale_id").alias("orders_cnt"),
        )
        .withColumn("avg_check", F.col("total_spent") / F.col("orders_cnt"))
    )
    write_to_clickhouse(
        by_customers.selectExpr(
            "customer_id",
            "customer_country",
            "full_name",
            "total_spent",
            "orders_cnt",
            "avg_check",
        ),
        "mart_sales_by_customers",
    )

    # 3) Sales by time (month)
    by_time = (
        fact.join(dim_date, on="date_key", how="left")
        .groupBy("year", "month")
        .agg(
            F.sum(F.col("total_price").cast("double")).alias("revenue"),
            F.countDistinct("sale_id").alias("orders_cnt"),
            F.avg(F.col("sale_quantity").cast("double")).alias("avg_order_size"),
        )
    )
    write_to_clickhouse(
        by_time.selectExpr(
            "year",
            "month",
            "revenue",
            "orders_cnt",
            "avg_order_size",
        ),
        "mart_sales_by_time",
    )

    # 4) Sales by stores
    by_stores = (
        fact.join(dim_store, on="store_key", how="left")
        .groupBy(
            "store_key",
            F.col("name").alias("store_name"),
            F.col("city").alias("store_city"),
            F.col("country").alias("store_country"),
        )
        .agg(
            F.sum(F.col("total_price").cast("double")).alias("revenue"),
            F.countDistinct("sale_id").alias("orders_cnt"),
        )
        .withColumn("avg_check", F.col("revenue") / F.col("orders_cnt"))
    )
    write_to_clickhouse(
        by_stores.selectExpr(
            "store_key",
            "store_name",
            "store_city",
            "store_country",
            "revenue",
            "orders_cnt",
            "avg_check",
        ),
        "mart_sales_by_stores",
    )

    # 5) Sales by suppliers
    by_suppliers = (
        fact.join(dim_supplier, on="supplier_key", how="left")
        .join(dim_product.select("product_id", "price"), on="product_id", how="left")
        .groupBy(
            "supplier_key",
            F.col("name").alias("supplier_name"),
            F.col("country").alias("supplier_country"),
        )
        .agg(
            F.sum(F.col("total_price").cast("double")).alias("revenue"),
            F.avg(F.col("price").cast("double")).alias("avg_price"),
            F.countDistinct("sale_id").alias("orders_cnt"),
        )
    )
    write_to_clickhouse(
        by_suppliers.selectExpr(
            "supplier_key",
            "supplier_name",
            "supplier_country",
            "revenue",
            "avg_price",
            "orders_cnt",
        ),
        "mart_sales_by_suppliers",
    )

    # 6) Product quality mart
    product_quality = (
        fact.join(dim_product, on="product_id", how="left")
        .groupBy(
            "product_id",
            F.col("name").alias("product_name"),
            F.col("category").alias("product_category"),
            F.col("rating").cast("double").alias("rating"),
            F.col("reviews").cast("long").alias("reviews_cnt"),
        )
        .agg(
            F.sum(F.col("sale_quantity").cast("long")).alias("sales_qty"),
            F.sum(F.col("total_price").cast("double")).alias("revenue"),
        )
    )
    write_to_clickhouse(
        product_quality.selectExpr(
            "product_id",
            "product_name",
            "product_category",
            "rating",
            "reviews_cnt",
            "sales_qty",
            "revenue",
        ),
        "mart_product_quality",
    )

    spark.stop()


if __name__ == "__main__":
    main()

