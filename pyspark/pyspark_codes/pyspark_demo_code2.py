from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, year, sum, avg, max

# Initialize Spark Session
spark = SparkSession.builder.appName("SalesDataAnalysis").getOrCreate()

# Sample Data
data = [
    (1, "Laptop", "Electronics", 60000, 2, "North", "2025-01-01"),
    (2, "Phone", "Electronics", 30000, 5, "South", "2025-01-02"),
    (3, "Tablet", "Electronics", 20000, 3, "East", "2025-01-02"),
    (4, "Laptop", "Electronics", 75000, 1, "West", "2025-01-03"),
    (5, "Phone", "Electronics", 25000, 4, "North", "2025-01-03"),
    (6, "Tablet", "Electronics", 15000, 2, "South", "2025-01-04"),
    (7, "Laptop", "Electronics", 55000, 1, "East", "2025-01-04"),
    (8, "Phone", "Electronics", 28000, 3, "West", "2025-01-05"),
]

# Define Schema
columns = ["SalesID", "Product", "Category", "SalesAmount", "Quantity", "Region", "SaleDate"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Transformation Task 1
# 1. Filter SalesAmount > 20000
filtered_df = df.filter(col("SalesAmount") >= 20000)

# 2. Add TotalSales column
transformed_df = filtered_df.withColumn("TotalSales", col("SalesAmount") * col("Quantity"))

# 3. Add SaleYear column
transformed_df = transformed_df.withColumn("SaleYear", year(col("SaleDate")))

# 4. Group by Product and calculate total SalesAmount, Quantity, and avg TotalSales
grouped_df = transformed_df.groupBy("Product").agg(
    sum("SalesAmount").alias("TotalSalesAmount"),
    sum("Quantity").alias("TotalQuantity"),
    avg("TotalSales").alias("AvgTotalSales")
)

# Display Grouped Results
print("Grouped by Product:")
grouped_df.show()

# Transformation Task 2
# 1. Find the region with the highest total SalesAmount
region_sales_df = transformed_df.groupBy("Region").agg(sum("SalesAmount").alias("TotalSalesAmount"))
max_sales_region = region_sales_df.orderBy(col("TotalSalesAmount").desc()).limit(1)
print("Region with highest total SalesAmount:")
max_sales_region.show()

# 2. Sort by SaleDate and SalesAmount descending
sorted_df = transformed_df.orderBy(col("SaleDate"), col("SalesAmount").desc())

print("Sorted Dataset:")
sorted_df.show()

# 3. Collect top 3 records
top_3_records = sorted_df.limit(3).collect()
print("Top 3 Records:")
for record in top_3_records:
    print(record)
