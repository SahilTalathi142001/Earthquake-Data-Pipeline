from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

# Read the JSON data
df = spark.read.option("multiline", "true").json(f"Files/{start_date}_earthquake_data.json")

# Extract and rename key attributes
df = df.select(
    'id',
    col('geometry.coordinates').getItem(0).alias('longitude'),
    col('geometry.coordinates').getItem(1).alias('latitude'),
    col('geometry.coordinates').getItem(2).alias('elevation'),
    col('properties.title').alias('title'),
    col('properties.place').alias('place_description'),
    col('properties.sig').alias('sig'),
    col('properties.mag').alias('mag'),
    col('properties.magType').alias('magType'),
    col('properties.time').alias('time'),
    col('properties.updated').alias('updated')
)

# Convert timestamps
df = df.withColumn('time', col('time')/1000).withColumn('updated', col('updated')/1000)
df = df.withColumn('time', col('time').cast(TimestampType())).withColumn('updated', col('updated').cast(TimestampType()))

# Save to Silver table
df.write.mode('append').saveAsTable('earthquake_events_silver')
