from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat_ws,
    corr,
    count,
    date_format,
    dayofweek,
    dense_rank,
    first,
    lit,
    month,
    row_number,
    sha2,
    sum as spark_sum,
    to_date,
    year,
)
from pyspark.sql.types import DoubleType, IntegerType, LongType


POSTGRES_URL = "jdbc:postgresql://postgres:5432/bd_spark"
POSTGRES_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/default"
CLICKHOUSE_PROPS = {
    "user": "bd_spark",
    "password": "bd_spark",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def execute_clickhouse(statements):
    for sql in statements:
        normalized_sql = " ".join(sql.split())
        query = urlencode(
            {
                "user": CLICKHOUSE_PROPS["user"],
                "password": CLICKHOUSE_PROPS["password"],
            }
        )
        request = Request(
            f"http://clickhouse:8123/?{query}",
            data=normalized_sql.encode("utf-8"),
            headers={"User-Agent": "spark-etl"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=30) as response:
                response.read()
        except HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ClickHouse query failed: {normalized_sql}\n{body}") from error


def make_key(columns):
    return sha2(
        concat_ws(
            "||",
            *[coalesce(col(column).cast("string"), lit("__NULL__")) for column in columns],
        ),
        256,
    )


def with_id(df, id_column, order_columns):
    window = Window.orderBy(*[col(column).asc_nulls_last() for column in order_columns])
    return df.withColumn(id_column, row_number().over(window).cast(LongType()))


def write_postgres(df, table):
    df.write.jdbc(
        url=POSTGRES_URL,
        table=table,
        mode="overwrite",
        properties=POSTGRES_PROPS,
    )


def write_clickhouse(df, table):
    df.write.jdbc(
        url=CLICKHOUSE_URL,
        table=table,
        mode="append",
        properties=CLICKHOUSE_PROPS,
    )


def main():
    spark = (
        SparkSession.builder.appName("big-data-spark-lab")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    source = (
        spark.read.jdbc(
            url=POSTGRES_URL,
            table="staging.mock_data",
            properties=POSTGRES_PROPS,
        )
        .withColumn("sale_date", to_date(col("sale_date")))
        .withColumn("product_release_date", to_date(col("product_release_date")))
        .withColumn("product_expiry_date", to_date(col("product_expiry_date")))
        .withColumn("sale_total_price", col("sale_total_price").cast(DoubleType()))
        .withColumn("product_price", col("product_price").cast(DoubleType()))
        .withColumn("product_weight", col("product_weight").cast(DoubleType()))
        .withColumn("product_rating", col("product_rating").cast(DoubleType()))
        .withColumn("sale_quantity", col("sale_quantity").cast(IntegerType()))
        .cache()
    )

    customer_cols = [
        "sale_customer_id",
        "customer_first_name",
        "customer_last_name",
        "customer_age",
        "customer_email",
        "customer_country",
        "customer_postal_code",
        "customer_pet_type",
        "customer_pet_name",
        "customer_pet_breed",
    ]
    seller_cols = [
        "sale_seller_id",
        "seller_first_name",
        "seller_last_name",
        "seller_email",
        "seller_country",
        "seller_postal_code",
    ]
    supplier_cols = [
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    ]
    store_cols = [
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email",
    ]
    product_cols = [
        "sale_product_id",
        "product_name",
        "product_category",
        "product_price",
        "product_quantity",
        "pet_category",
        "product_weight",
        "product_color",
        "product_size",
        "product_brand",
        "product_material",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date",
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country",
    ]

    source = (
        source.withColumn("customer_key", make_key(customer_cols))
        .withColumn("seller_key", make_key(seller_cols))
        .withColumn("supplier_key", make_key(supplier_cols))
        .withColumn("store_key", make_key(store_cols))
        .withColumn("product_key", make_key(product_cols))
    )

    dim_customer = with_id(
        source.select("customer_key", *customer_cols).distinct(),
        "customer_id",
        ["customer_email", "sale_customer_id", "customer_key"],
    )
    dim_seller = with_id(
        source.select("seller_key", *seller_cols).distinct(),
        "seller_id",
        ["seller_email", "sale_seller_id", "seller_key"],
    )
    dim_supplier = with_id(
        source.select("supplier_key", *supplier_cols).distinct(),
        "supplier_id",
        ["supplier_name", "supplier_email", "supplier_key"],
    )
    dim_store = with_id(
        source.select("store_key", *store_cols).distinct(),
        "store_id",
        ["store_name", "store_city", "store_key"],
    )
    dim_product_category = with_id(
        source.select("product_category", "pet_category").distinct(),
        "product_category_id",
        ["product_category", "pet_category"],
    )

    dim_product = with_id(
        source.select("product_key", *product_cols).distinct(),
        "product_id",
        ["product_name", "sale_product_id", "product_key"],
    )

    dim_date = (
        source.select("sale_date")
        .where(col("sale_date").isNotNull())
        .distinct()
        .withColumn("date_id", date_format(col("sale_date"), "yyyyMMdd").cast(IntegerType()))
        .withColumn("year_number", year(col("sale_date")))
        .withColumn("month_number", month(col("sale_date")))
        .withColumn("month_name", date_format(col("sale_date"), "MMMM"))
        .withColumn("day_of_month", date_format(col("sale_date"), "d").cast(IntegerType()))
        .withColumn("day_of_week", dayofweek(col("sale_date")).cast(IntegerType()))
    )

    fact_sales = (
        source.select(
            "source_file",
            "id",
            "sale_date",
            "customer_key",
            "seller_key",
            "supplier_key",
            "store_key",
            "product_key",
            "sale_quantity",
            "sale_total_price",
        )
        .join(dim_customer.select("customer_key", "customer_id"), "customer_key")
        .join(dim_seller.select("seller_key", "seller_id"), "seller_key")
        .join(dim_supplier.select("supplier_key", "supplier_id"), "supplier_key")
        .join(dim_store.select("store_key", "store_id"), "store_key")
        .join(dim_product.select("product_key", "product_id"), "product_key")
        .join(dim_date.select("sale_date", "date_id"), "sale_date")
        .select(
            "source_file",
            col("id").alias("source_row_id"),
            "date_id",
            "customer_id",
            "seller_id",
            "supplier_id",
            "store_id",
            "product_id",
            "sale_quantity",
            "sale_total_price",
        )
        .withColumn("sale_id", row_number().over(Window.orderBy("source_file", "source_row_id")))
    )

    write_postgres(dim_customer, "mart.dim_customer")
    write_postgres(dim_seller, "mart.dim_seller")
    write_postgres(dim_supplier, "mart.dim_supplier")
    write_postgres(dim_store, "mart.dim_store")
    write_postgres(dim_product_category, "mart.dim_product_category")
    write_postgres(dim_product, "mart.dim_product")
    write_postgres(dim_date, "mart.dim_date")
    write_postgres(fact_sales, "mart.fact_sales")

    mart = (
        fact_sales.alias("fs")
        .join(dim_product.alias("p"), "product_id")
        .join(dim_customer.alias("c"), "customer_id")
        .join(dim_store.alias("st"), "store_id")
        .join(dim_supplier.alias("sup"), "supplier_id")
        .join(dim_date.alias("d"), "date_id")
        .select(
            col("fs.sale_id"),
            col("fs.source_file"),
            col("fs.source_row_id"),
            col("fs.sale_quantity"),
            col("fs.sale_total_price"),
            col("p.product_id"),
            col("p.product_name"),
            col("p.product_category"),
            col("p.product_price"),
            col("p.product_rating"),
            col("p.product_reviews"),
            col("c.customer_id"),
            col("c.customer_first_name"),
            col("c.customer_last_name"),
            col("c.customer_email"),
            col("c.customer_country"),
            col("st.store_id"),
            col("st.store_name"),
            col("st.store_city"),
            col("st.store_state"),
            col("st.store_country"),
            col("sup.supplier_id"),
            col("sup.supplier_name"),
            col("sup.supplier_city"),
            col("sup.supplier_country"),
            col("d.year_number"),
            col("d.month_number"),
            col("d.month_name"),
        )
    ).cache()

    product_sales = (
        mart.groupBy("product_id", "product_name", "product_category")
        .agg(
            spark_sum("sale_quantity").cast(LongType()).alias("total_quantity"),
            spark_sum("sale_total_price").alias("total_revenue"),
            count("*").cast(LongType()).alias("sales_count"),
            avg("product_rating").alias("avg_rating"),
            first("product_reviews").cast(LongType()).alias("reviews_count"),
        )
        .withColumn("sales_rank", dense_rank().over(Window.orderBy(col("total_quantity").desc())))
    )

    customer_sales = mart.groupBy(
        "customer_id",
        "customer_first_name",
        "customer_last_name",
        "customer_email",
        "customer_country",
    ).agg(
        count("*").cast(LongType()).alias("orders_count"),
        spark_sum("sale_quantity").cast(LongType()).alias("total_quantity"),
        spark_sum("sale_total_price").alias("total_spent"),
        avg("sale_total_price").alias("avg_order_amount"),
    )

    time_sales = mart.groupBy("year_number", "month_number", "month_name").agg(
        count("*").cast(LongType()).alias("orders_count"),
        spark_sum("sale_quantity").cast(LongType()).alias("total_quantity"),
        spark_sum("sale_total_price").alias("total_revenue"),
        avg("sale_total_price").alias("avg_order_amount"),
    )

    store_sales = mart.groupBy(
        "store_id", "store_name", "store_city", "store_state", "store_country"
    ).agg(
        count("*").cast(LongType()).alias("orders_count"),
        spark_sum("sale_quantity").cast(LongType()).alias("total_quantity"),
        spark_sum("sale_total_price").alias("total_revenue"),
        avg("sale_total_price").alias("avg_order_amount"),
    )

    supplier_sales = mart.groupBy(
        "supplier_id", "supplier_name", "supplier_city", "supplier_country"
    ).agg(
        count("*").cast(LongType()).alias("orders_count"),
        spark_sum("sale_quantity").cast(LongType()).alias("total_quantity"),
        spark_sum("sale_total_price").alias("total_revenue"),
        avg("product_price").alias("avg_product_price"),
    )

    rating_correlation = mart.select(corr("product_rating", "sale_quantity")).first()[0]
    product_quality = mart.groupBy(
        "product_id", "product_name", "product_category", "product_rating"
    ).agg(
        first("product_reviews").cast(LongType()).alias("reviews_count"),
        spark_sum("sale_quantity").cast(LongType()).alias("total_quantity"),
        spark_sum("sale_total_price").alias("total_revenue"),
    ).withColumn(
        "rating_sales_correlation",
        lit(float(rating_correlation or 0.0)),
    )

    clickhouse_ddl = [
        "DROP TABLE IF EXISTS report_product_sales",
        "DROP TABLE IF EXISTS report_customer_sales",
        "DROP TABLE IF EXISTS report_time_sales",
        "DROP TABLE IF EXISTS report_store_sales",
        "DROP TABLE IF EXISTS report_supplier_sales",
        "DROP TABLE IF EXISTS report_product_quality",
        """
        CREATE TABLE report_product_sales (
            product_id Int64,
            product_name String,
            product_category Nullable(String),
            total_quantity Int64,
            total_revenue Float64,
            sales_count Int64,
            avg_rating Nullable(Float64),
            reviews_count Nullable(Int64),
            sales_rank Int32
        ) ENGINE = MergeTree ORDER BY (sales_rank, product_id)
        """,
        """
        CREATE TABLE report_customer_sales (
            customer_id Int64,
            customer_first_name Nullable(String),
            customer_last_name Nullable(String),
            customer_email Nullable(String),
            customer_country Nullable(String),
            orders_count Int64,
            total_quantity Int64,
            total_spent Float64,
            avg_order_amount Float64
        ) ENGINE = MergeTree ORDER BY (total_spent, customer_id)
        """,
        """
        CREATE TABLE report_time_sales (
            year_number Int32,
            month_number Int32,
            month_name String,
            orders_count Int64,
            total_quantity Int64,
            total_revenue Float64,
            avg_order_amount Float64
        ) ENGINE = MergeTree ORDER BY (year_number, month_number)
        """,
        """
        CREATE TABLE report_store_sales (
            store_id Int64,
            store_name Nullable(String),
            store_city Nullable(String),
            store_state Nullable(String),
            store_country Nullable(String),
            orders_count Int64,
            total_quantity Int64,
            total_revenue Float64,
            avg_order_amount Float64
        ) ENGINE = MergeTree ORDER BY (total_revenue, store_id)
        """,
        """
        CREATE TABLE report_supplier_sales (
            supplier_id Int64,
            supplier_name Nullable(String),
            supplier_city Nullable(String),
            supplier_country Nullable(String),
            orders_count Int64,
            total_quantity Int64,
            total_revenue Float64,
            avg_product_price Float64
        ) ENGINE = MergeTree ORDER BY (total_revenue, supplier_id)
        """,
        """
        CREATE TABLE report_product_quality (
            product_id Int64,
            product_name String,
            product_category Nullable(String),
            product_rating Nullable(Float64),
            reviews_count Nullable(Int64),
            total_quantity Int64,
            total_revenue Float64,
            rating_sales_correlation Float64
        ) ENGINE = MergeTree ORDER BY product_id
        """,
    ]
    execute_clickhouse(clickhouse_ddl)

    write_clickhouse(product_sales, "report_product_sales")
    write_clickhouse(customer_sales, "report_customer_sales")
    write_clickhouse(time_sales, "report_time_sales")
    write_clickhouse(store_sales, "report_store_sales")
    write_clickhouse(supplier_sales, "report_supplier_sales")
    write_clickhouse(product_quality, "report_product_quality")

    print("Spark ETL finished successfully")
    print(f"staging rows: {source.count()}")
    print(f"fact rows: {fact_sales.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
