from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("QueryParquetOutput") \
    .master("local[*]") \
    .getOrCreate()

# Read the output Parquet data
df = spark.read.parquet("output_samples/parquet")

# Register temporary view for SQL queries
df.createOrReplaceTempView("config_events")

# 1️⃣ Most popular car models
print("🔍 Most Popular Car Models:")
spark.sql("""
    SELECT model, COUNT(*) AS event_count
    FROM config_events
    GROUP BY model
    ORDER BY event_count DESC
""").show(truncate=False)

# 2️⃣ Average build price by engine type
print("💸 Average Price by Engine:")
spark.sql("""
    SELECT engine, ROUND(AVG(price), 2) AS avg_price
    FROM config_events
    GROUP BY engine
    ORDER BY avg_price DESC
""").show(truncate=False)

# 3️⃣ Most selected color per model
print("🎨 Top Colors Per Model:")
spark.sql("""
    SELECT model, color, COUNT(*) AS color_count
    FROM config_events
    GROUP BY model, color
    ORDER BY model, color_count DESC
""").show(truncate=False)

# 4️⃣ Most common configuration step
print("🪜 Most Common Configurator Steps:")
spark.sql("""
    SELECT step, COUNT(*) AS step_count
    FROM config_events
    GROUP BY step
    ORDER BY step_count DESC
""").show(truncate=False)

# 5️⃣ Distribution of prices (optional: histogram buckets)
print("📊 Price Distribution Buckets:")
spark.sql("""
    SELECT 
        CASE 
            WHEN price < 70000 THEN '<70k'
            WHEN price BETWEEN 70000 AND 90000 THEN '70k–90k'
            WHEN price BETWEEN 90000 AND 110000 THEN '90k–110k'
            ELSE '>110k'
        END AS price_range,
        COUNT(*) AS count
    FROM config_events
    GROUP BY price_range
    ORDER BY price_range
""").show(truncate=False)

# Stop the Spark session
spark.stop()
