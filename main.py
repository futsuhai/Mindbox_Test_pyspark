from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def process_data(spark):
    
    # Получаем DataFrames
    df_products = spark.read.format("csv").option("header", "true").load("products.csv")
    df_category = spark.read.format("csv").option("header", "true").load("categories.csv")
    df_connection = spark.read.format("csv").option("header", "true").load("connection.csv")
    
    # Делаем соединение данных
    joined_data = df_products.join(df_connection, df_products["product_id"] == df_connection["product_id"], "left") \
                              .join(df_category, df_connection["category_id"] == df_category["category_id"], "left")

    # Получаем пары "Имя продукта – Имя категории"
    product_category_pairs = joined_data.select(df_products["product_name"], df_category["category_name"]) \
                                        .where(col("category_name").isNotNull())
                                        
    # Получаем имена продуктов без категорий
    products_without_categories = df_products.join(df_connection, df_products["product_id"] == df_connection["product_id"], "left") \
                                        .filter(df_connection["category_id"].isNull()) \
                                        .select(df_products["product_name"].alias("product_name"), lit(None).alias("category_name"))
    
    df_result = product_category_pairs.unionAll(products_without_categories)

    return df_result
    
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Test PySpark") \
        .getOrCreate()

    df_result = process_data(spark)
    df_result.show()

    spark.stop()

