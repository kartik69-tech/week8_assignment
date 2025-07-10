from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, to_date, unix_timestamp, max as _max, count
from pyspark.sql.window import Window
import pandas as pd

# 1. Create Spark Session
spark = SparkSession.builder \
    .appName("NYC Taxi Excel Dataset Analysis") \
    .getOrCreate()

# 2. Load Excel using Pandas
excel_df = pd.read_excel(r"C:\Users\mathu\OneDrive\Desktop\Assignment_8\yellow_tripdata_2020_01_synthetic.xlsx", engine='openpyxl')


# 3. Convert Pandas DataFrame to Spark DataFrame
df = spark.createDataFrame(excel_df)

# 4. Add Revenue Column
df = df.withColumn("Revenue", col("fare_amount") + col("extra") + col("mta_tax") +
                   col("improvement_surcharge") + col("tip_amount") + col("tolls_amount") +
                   col("total_amount"))

# 5. Passenger count by pickup area
df.groupBy("PULocationID").agg(_sum("passenger_count").alias("Total_Passengers")).show()

# 6. Real-time average fare/earnings by vendor
df.groupBy("VendorID").agg(
    _sum("fare_amount").alias("Total_Fare"),
    _sum("total_amount").alias("Total_Earnings")
).show()

# 7. Moving count of payments
windowSpec = Window.partitionBy("payment_type").orderBy("tpep_pickup_datetime").rowsBetween(Window.unboundedPreceding, 0)
df.withColumn("Cumulative_Count", count("payment_type").over(windowSpec)).show()

# 8. Top 2 vendors by earnings on a date
df_date = df.withColumn("trip_date", to_date("tpep_pickup_datetime"))
df_date.groupBy("trip_date", "VendorID") \
    .agg(_sum("total_amount").alias("Earning"),
         _sum("passenger_count").alias("Passengers"),
         _sum("trip_distance").alias("Distance")) \
    .filter(col("trip_date") == "2020-01-15") \
    .orderBy(col("Earning").desc()) \
    .show(2)

# 9. Route with most passengers
df.groupBy("PULocationID", "DOLocationID") \
  .agg(_sum("passenger_count").alias("Total_Passengers")) \
  .orderBy(col("Total_Passengers").desc()) \
  .show(1)

# 10. Top pickup in last 10 seconds (from latest timestamp)
latest_ts = df.select(unix_timestamp("tpep_pickup_datetime").alias("ts")).agg(_max("ts")).first()["max(ts)"]
df.filter((unix_timestamp("tpep_pickup_datetime") >= (latest_ts - 10)) &
          (unix_timestamp("tpep_pickup_datetime") <= latest_ts)) \
    .groupBy("PULocationID") \
    .agg(_sum("passenger_count").alias("Recent_Passengers")) \
    .orderBy(col("Recent_Passengers").desc()) \
    .show()

# 11. Save to Parquet
df.write.mode("overwrite").parquet("output/yellow_tripdata_2020_01_parquet")

spark.stop()
