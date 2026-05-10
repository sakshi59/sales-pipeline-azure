# Databricks notebook source
# Cell 1 — Mount Azure Data Lake Storage to Databricks

storage_account_name = "stgsalespipelinesv"
storage_account_key  = "YOUR_STORAGE_ACCOUNT_KEY_HERE"
container_name       = "processed"

mount_point = "/mnt/salespipeline"

# Check if already mounted — unmount first if so
existing_mounts = [m.mountPoint for m in dbutils.fs.mounts()]
if mount_point in existing_mounts:
    dbutils.fs.unmount(mount_point)

# Mount the processed container
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point = mount_point,
    extra_configs = {
        f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key
    }
)

print("Storage mounted successfully at /mnt/salespipeline")

# COMMAND ----------

# Cell 2 — Read the CSV file into a Spark DataFrame

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/mnt/salespipeline/superstore_processed.csv")

# Show the first 5 rows
df_raw.show(5)

# Print total number of rows
print(f"Total rows: {df_raw.count()}")

# Print the schema — column names and data types
df_raw.printSchema()

# COMMAND ----------

# Cell 3 — Data profiling before transformation

from pyspark.sql.functions import col, count, when, isnan

print("=== SHAPE ===")
print(f"Rows: {df_raw.count()}")
print(f"Columns: {len(df_raw.columns)}")

print("\n=== NULL VALUES PER COLUMN ===")
df_raw.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_raw.columns
]).show()

print("\n=== DUPLICATE ROWS ===")
total = df_raw.count()
distinct = df_raw.distinct().count()
print(f"Total rows: {total}")
print(f"Distinct rows: {distinct}")
print(f"Duplicate rows: {total - distinct}")

print("\n=== SAMPLE VALUES FOR PROBLEM COLUMNS ===")
df_raw.select("Sales", "Quantity", "Discount").show(5)

# COMMAND ----------

# Cell 4 — Clean and transform the data

from pyspark.sql.functions import col, round, to_date

# Step 1 — Fix data types
df_cleaned = df_raw \
    .withColumn("Sales",    col("Sales").cast("double")) \
    .withColumn("Quantity", col("Quantity").cast("integer")) \
    .withColumn("Discount", col("Discount").cast("double"))

# Step 2 — Rename columns to remove spaces
# Spaces in column names cause problems in SQL and downstream tools
df_cleaned = df_cleaned \
    .withColumnRenamed("Row ID",       "Row_ID") \
    .withColumnRenamed("Order ID",     "Order_ID") \
    .withColumnRenamed("Order Date",   "Order_Date") \
    .withColumnRenamed("Ship Date",    "Ship_Date") \
    .withColumnRenamed("Ship Mode",    "Ship_Mode") \
    .withColumnRenamed("Customer ID",  "Customer_ID") \
    .withColumnRenamed("Customer Name","Customer_Name") \
    .withColumnRenamed("Postal Code",  "Postal_Code") \
    .withColumnRenamed("Product ID",   "Product_ID") \
    .withColumnRenamed("Product Name", "Product_Name") \
    .withColumnRenamed("Sub-Category", "Sub_Category")

# Step 3 — Add calculated columns
df_cleaned = df_cleaned \
    .withColumn("Revenue",       round(col("Sales") * col("Quantity"), 2)) \
    .withColumn("Profit_Margin", round((col("Profit") / col("Sales")) * 100, 2))

# Step 4 — Reorder columns cleanly
df_cleaned = df_cleaned.select(
    "Row_ID", "Order_ID", "Order_Date", "Ship_Date", "Ship_Mode",
    "Customer_ID", "Customer_Name", "Segment", "Country", "City",
    "State", "Postal_Code", "Region", "Product_ID", "Category",
    "Sub_Category", "Product_Name", "Sales", "Quantity", "Discount",
    "Profit", "Revenue", "Profit_Margin"
)

# Preview the result
df_cleaned.show(5)
print(f"Transformed rows: {df_cleaned.count()}")
df_cleaned.printSchema()

# COMMAND ----------

# Cell 5 — Data validation checks

print("=== DATA VALIDATION REPORT ===\n")

# Check 1 — Null check after transformation
print("CHECK 1: Null values after transformation")
from pyspark.sql.functions import col, count, when

null_counts = df_cleaned.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_cleaned.columns
]).collect()[0].asDict()

failed_nulls = {k: v for k, v in null_counts.items() if v > 0}
if failed_nulls:
    print(f"FAILED — Nulls found: {failed_nulls}")
else:
    print("PASSED — No null values found")

# Check 2 — Negative sales check
print("\nCHECK 2: Negative Sales values")
negative_sales = df_cleaned.filter(col("Sales") < 0).count()
if negative_sales > 0:
    print(f"FAILED — {negative_sales} rows have negative Sales")
else:
    print("PASSED — No negative Sales values")

# Check 3 — Quantity must be positive
print("\nCHECK 3: Invalid Quantity values")
invalid_qty = df_cleaned.filter(col("Quantity") <= 0).count()
if invalid_qty > 0:
    print(f"FAILED — {invalid_qty} rows have zero or negative Quantity")
else:
    print("PASSED — All Quantity values are positive")

# Check 4 — Discount must be between 0 and 1
print("\nCHECK 4: Discount range check (must be 0-1)")
invalid_discount = df_cleaned.filter(
    (col("Discount") < 0) | (col("Discount") > 1)
).count()
if invalid_discount > 0:
    print(f"FAILED — {invalid_discount} rows have invalid Discount")
else:
    print("PASSED — All Discount values are within valid range")

# Check 5 — Row count must match source
print("\nCHECK 5: Row count integrity")
expected_rows = 9994
actual_rows = df_cleaned.count()
if actual_rows == expected_rows:
    print(f"PASSED — Row count matches source: {actual_rows}")
else:
    print(f"FAILED — Expected {expected_rows}, got {actual_rows}")

print("\n=== VALIDATION COMPLETE ===")

# COMMAND ----------

# Cell 6 — Investigate data quality issues

print("=== INVESTIGATING NULL SOURCES ===")

# Check what values in original data couldn't be cast
print("\nSample problematic Sales values from raw data:")
df_raw.filter(col("Sales").cast("double").isNull()) \
    .select("Sales") \
    .show(10)

print("\nSample problematic Quantity values:")
df_raw.filter(col("Quantity").cast("integer").isNull()) \
    .select("Quantity") \
    .show(10)

print("\nSample problematic Discount values:")
df_raw.filter(col("Discount").cast("double").isNull()) \
    .select("Discount") \
    .show(10)

print("\n=== INVESTIGATING INVALID DISCOUNTS ===")
print("Sample rows where Discount is outside 0-1 range:")
df_cleaned.filter(
    (col("Discount") < 0) | (col("Discount") > 1)
).select("Discount").distinct().show(20)

# COMMAND ----------

# Cell 7 — Remove corrupted rows and fix discount values

from pyspark.sql.functions import col, round, regexp_replace

print(f"Rows before cleaning: {df_cleaned.count()}")

# Step 1 — Remove rows where Sales Quantity or Discount 
# could not be cast to numeric (these are misaligned rows)
df_fixed = df_raw \
    .filter(col("Sales").cast("double").isNotNull()) \
    .filter(col("Quantity").cast("integer").isNotNull()) \
    .filter(col("Discount").cast("double").isNotNull())

print(f"Rows after removing misaligned rows: {df_fixed.count()}")

# Step 2 — Now apply type casting on clean rows
df_fixed = df_fixed \
    .withColumn("Sales",    col("Sales").cast("double")) \
    .withColumn("Quantity", col("Quantity").cast("integer")) \
    .withColumn("Discount", col("Discount").cast("double"))

# Step 3 — Fix discount values greater than 1
# These appear to be stored as percentages — divide by 100
df_fixed = df_fixed.withColumn(
    "Discount",
    when(col("Discount") > 1, round(col("Discount") / 100, 2))
    .otherwise(col("Discount"))
)

# Step 4 — Rename columns
df_fixed = df_fixed \
    .withColumnRenamed("Row ID",       "Row_ID") \
    .withColumnRenamed("Order ID",     "Order_ID") \
    .withColumnRenamed("Order Date",   "Order_Date") \
    .withColumnRenamed("Ship Date",    "Ship_Date") \
    .withColumnRenamed("Ship Mode",    "Ship_Mode") \
    .withColumnRenamed("Customer ID",  "Customer_ID") \
    .withColumnRenamed("Customer Name","Customer_Name") \
    .withColumnRenamed("Postal Code",  "Postal_Code") \
    .withColumnRenamed("Product ID",   "Product_ID") \
    .withColumnRenamed("Product Name", "Product_Name") \
    .withColumnRenamed("Sub-Category", "Sub_Category")

# Step 5 — Add calculated columns
df_fixed = df_fixed \
    .withColumn("Revenue",
        round(col("Sales") * col("Quantity"), 2)) \
    .withColumn("Profit_Margin",
        round((col("Profit") / col("Sales")) * 100, 2))

# Step 6 — Reorder columns
df_fixed = df_fixed.select(
    "Row_ID", "Order_ID", "Order_Date", "Ship_Date", "Ship_Mode",
    "Customer_ID", "Customer_Name", "Segment", "Country", "City",
    "State", "Postal_Code", "Region", "Product_ID", "Category",
    "Sub_Category", "Product_Name", "Sales", "Quantity", "Discount",
    "Profit", "Revenue", "Profit_Margin"
)

print(f"\nFinal clean rows: {df_fixed.count()}")
df_fixed.show(5)

# COMMAND ----------

# Cell 8 — Re-run validation on fixed data

print("=== FINAL VALIDATION REPORT ===\n")

# Check 1 - Nulls
print("CHECK 1: Null values")
null_counts = df_fixed.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_fixed.columns
]).collect()[0].asDict()
failed_nulls = {k: v for k, v in null_counts.items() if v > 0}
if failed_nulls:
    print("FAILED - Nulls found: " + str(failed_nulls))
else:
    print("PASSED - No null values")

# Check 2 - Negative Sales
print("\nCHECK 2: Negative Sales")
negative_sales = df_fixed.filter(col("Sales") < 0).count()
print("PASSED" if negative_sales == 0 else "FAILED - " + str(negative_sales) + " rows")

# Check 3 - Quantity positive
print("\nCHECK 3: Quantity values")
invalid_qty = df_fixed.filter(col("Quantity") <= 0)

# COMMAND ----------

# Cell 9 — Write clean data to curated layer as Parquet

# Mount the curated container
curated_mount = "/mnt/salespipeline_curated"

existing_mounts = [m.mountPoint for m in dbutils.fs.mounts()]
if curated_mount in existing_mounts:
    dbutils.fs.unmount(curated_mount)

dbutils.fs.mount(
    source = "wasbs://curated@stgsalespipelinesv.blob.core.windows.net",
    mount_point = curated_mount,
    extra_configs = {
        "fs.azure.account.key.stgsalespipelinesv.blob.core.windows.net": 
        storage_account_key
    }
)

print("Curated container mounted successfully")

# Write df_fixed to curated layer as Parquet
df_fixed.write \
    .mode("overwrite") \
    .parquet(curated_mount + "/superstore_curated")

print("Data written to curated layer successfully")
print("Location: /mnt/salespipeline_curated/superstore_curated")

# Verify — read it back and count rows
df_verify = spark.read.parquet(
    curated_mount + "/superstore_curated"
)
print("Verification row count: " + str(df_verify.count()))
