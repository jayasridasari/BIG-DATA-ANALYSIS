from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, year, desc
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Fare Analysis") \
    .getOrCreate()

# Load dataset
data_path = "/content/train.csv"  # Adjust path if needed
df = spark.read.option("header", True).option("inferSchema", True).csv(data_path)

# Preview data
print("Schema:")
df.printSchema()

print("Sample records:")
df.show(5)

# Basic cleaning: Remove nulls and unreasonable fares
df_clean = df.dropna() \
    .filter((col("fare_amount") > 0) & (col("fare_amount") < 200))

# Extract datetime features
df_clean = df_clean.withColumn("hour", hour("pickup_datetime")) \
                   .withColumn("day", dayofweek("pickup_datetime")) \
                   .withColumn("month", month("pickup_datetime")) \
                   .withColumn("year", year("pickup_datetime"))

# Show cleaned data
print("Cleaned Data Sample:")
df_clean.select("fare_amount", "hour", "day", "month", "year").show(5)

# Basic analysis: Average fare by hour
print("Average fare by hour:")
avg_fare_by_hour = df_clean.groupBy("hour").avg("fare_amount").orderBy("hour")
avg_fare_by_hour.show()

# Insight 2: Average fare by day of week
print("Average fare by day of week:")
avg_fare_by_day = df_clean.groupBy("day").avg("fare_amount").orderBy("day")
avg_fare_by_day.show()

# Save output to file
output_path = "output/avg_fare_by_hour.csv"
avg_fare_by_hour.write.mode("overwrite").csv(output_path, header=True)

print(f"\nAnalysis complete. Results saved to: {output_path}")

# Stop Spark session
spark.stop()
