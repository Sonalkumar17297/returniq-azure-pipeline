# ============================================
# ReturnIQ — Notebook 1: Bronze to Silver to Gold
# ============================================

from pyspark.sql.functions import col, count, sum, round, when, to_date

# ============================================
# Step 1 — Set up ADLS connection
# ============================================
storage_account_name = "returniqadls"
storage_account_key = dbutils.secrets.get(
    scope="returniq-scope",
    key="adls-access-key"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# ============================================
# Step 2 — Read raw CSV from Bronze
# ============================================
bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/ecommerce_returns_synthetic_data.csv"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(bronze_path)

print("Raw record count:", df.count())
df.printSchema()

# ============================================
# Step 3 — Clean and Transform (Bronze to Silver)
# ============================================
df = df.dropDuplicates()
df = df.fillna({"Return_Date": "9999-12-31", "Days_to_Return": 0})

df = df.withColumn("Order_Date", to_date(col("Order_Date"))) \
       .withColumn("Return_Date", to_date(col("Return_Date"))) \
       .withColumn("Product_Price", col("Product_Price").cast("double")) \
       .withColumn("Order_Quantity", col("Order_Quantity").cast("integer")) \
       .withColumn("Days_to_Return", col("Days_to_Return").cast("integer")) \
       .withColumn("Discount_Applied", col("Discount_Applied").cast("double"))

df = df.withColumn("Is_Returned",
    when(col("Return_Status") == "Returned", 1).otherwise(0))

print("Clean record count:", df.count())

# ============================================
# Step 4 — Write to Silver
# ============================================
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/returns_cleaned"

df.write.format("parquet") \
    .mode("overwrite") \
    .save(silver_path)

print("✅ Silver layer written successfully!")

# ============================================
# Step 5 — Business Logic (Silver to Gold)
# ============================================
df_silver = spark.read.parquet(silver_path)

user_return_rate = df_silver.groupBy("User_ID").agg(
    count("Order_ID").alias("Total_Orders"),
    sum("Is_Returned").alias("Total_Returns")
).withColumn(
    "Return_Rate", round(col("Total_Returns") / col("Total_Orders") * 100, 2)
).withColumn(
    "Is_Serial_Returner", when(col("Return_Rate") >= 40, 1).otherwise(0)
)

category_return_rate = df_silver.groupBy("Product_Category").agg(
    count("Order_ID").alias("Total_Orders"),
    sum("Is_Returned").alias("Total_Returns")
).withColumn(
    "Category_Return_Rate", round(col("Total_Returns") / col("Total_Orders") * 100, 2)
)

# ============================================
# Step 6 — Write to Gold
# ============================================
gold_path_users = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/user_return_rates"
gold_path_category = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/category_return_rates"

user_return_rate.write.format("parquet").mode("overwrite").save(gold_path_users)
category_return_rate.write.format("parquet").mode("overwrite").save(gold_path_category)

print("✅ Gold layer written successfully!")