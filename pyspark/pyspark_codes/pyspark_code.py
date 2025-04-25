from pyspark.sql import SparkSession
from pyspark.sql import StorageLevel 

spark = SparkSession.builder.appName("example").getOrCreate()

df = spark.range(10)
print(type(df))
df.printSchema()

intial_storage_level = df.storageLevel = df.rdd.getStorageLevel()
print("instila_storage_level:", intial_storage_level)

current_storage_level = df.rdd.getStorageLevel()
df.show()
df.select("id",df["id"]+5).show()
df.select("id").show()
df.select("id",(df["id"]+5).alias("new_id")).show()
df.select(df.id + 5).show()