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

def to_snake_case(name):
    # Split acronyms followed by a capitalised word (e.g., "ISOFormat" -> "ISO_Format")
    s1 = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name.strip())
    # Split lowercase letters or numbers followed by an uppercase letter (e.g., "holidayName" -> "holiday_Name")
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    # Replace spaces, hyphens, or symbols with a single underscore and lowercase everything
    return re.sub(r'[^a-z0-9]+', '_', s2.lower()).strip('_')

# 1. Read from the raw/bronze sample table
df = spark.sql("SELECT * FROM lakehouse_playground.dbo.publicholidays LIMIT 1000")

# 2. Clean the column names dynamically
df_clean = df.toDF(*[to_snake_case(col) for col in df.columns])

# 3. Write to the Silver Delta table
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lakehouse_playground.dbo.silver_public_holidays")

# 4. Verify the updated schema and data
df_clean.printSchema()
display(spark.sql("SELECT * FROM lakehouse_playground.dbo.silver_public_holidays LIMIT 10"))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
