from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import StringType
import reverse_geocoder as rg

# Read data from Silver table
df = spark.read.table("earthquake_events_silver").filter(col('time') > start_date)

# Define UDF for reverse geocoding
def get_country_code(lat, lon):
    coordinates = (float(lat), float(lon))
    return rg.search(coordinates)[0].get('cc')

get_country_code_udf = udf(get_country_code, StringType())

# Add country code and significance classification
df_with_location = df.withColumn("country_code", get_country_code_udf(col("latitude"), col("longitude")))
df_with_location_sig_class = df_with_location.withColumn('sig_class', 
    when(col("sig") < 100, "Low")
    .when((col("sig") >= 100) & (col("sig") < 500), "Moderate")
    .otherwise("High")
)

# Save to Gold table
df_with_location_sig_class.write.mode('append').saveAsTable('earthquake_events_gold')
