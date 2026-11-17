# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d7cb86ab-afda-47a8-8524-0c3ac9c31ea7",
# META       "default_lakehouse_name": "lakehouse_playground",
# META       "default_lakehouse_workspace_id": "2feb7682-1b3e-4cd7-aaf1-68d8c6b46f4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "d7cb86ab-afda-47a8-8524-0c3ac9c31ea7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import re
from pyspark.sql import functions as F

def to_snake_case(name):
    s1 = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name.strip())
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    return re.sub(r'[^a-z0-9]+', '_', s2.lower()).strip('_')

env_vars = notebookutils.variableLibrary.getLibrary("env_config")
df = spark.sql(f"SELECT * FROM dbo.publicholidays {env_vars.row_limit}")

# Clean column names and add a new feature column: day_of_week
df_clean = (
    df.toDF(*[to_snake_case(col) for col in df.columns])
      .withColumn("day_of_week", F.date_format(F.col("date"), "EEEE"))
)

df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dbo.silver_public_holidays")

display(spark.sql("SELECT date, day_of_week, holiday_name FROM dbo.silver_public_holidays LIMIT 10"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
