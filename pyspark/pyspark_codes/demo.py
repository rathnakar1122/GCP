import pyspark
from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
import os


# Set environment variables if needed
os.environ["JAVA_HOME"] = "C:\Program Files\Eclipse Foundation\jdk-11.0.12.7-hotspot\bin\java.exe"
os.environ["SPARK_HOME"] = "C:\ENV\myenv\Scripts\pyspark"

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Demo").getOrCreate()
print("SparkSession successfully created!")


# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("DataFrame Example") \
    .getOrCreate()

# Create a DataFrame

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

# Show the DataFrame
df.show()
