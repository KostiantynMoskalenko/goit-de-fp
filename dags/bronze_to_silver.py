import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

spark = SparkSession.builder.appName("Bronze_to_Silver").getOrCreate()

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    df = spark.read.parquet(f"/tmp/bronze/{table}")
    text_columns = [field.name for field in df.schema.fields if str(field.dataType) == "StringType"]
    for col_name in text_columns:
        df = df.withColumn(col_name, clean_text_udf(col(col_name)))
    df = df.dropDuplicates()
    output_path = f"/tmp/silver/{table}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

spark.stop()
