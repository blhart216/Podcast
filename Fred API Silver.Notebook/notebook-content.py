# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f80cb5d5-5742-4adc-a99c-f30bee868bb7",
# META       "default_lakehouse_name": "Housing_Bronze",
# META       "default_lakehouse_workspace_id": "85987632-562b-4a33-86f0-957bbdc9bc53",
# META       "known_lakehouses": [
# META         {
# META           "id": "f80cb5d5-5742-4adc-a99c-f30bee868bb7"
# META         },
# META         {
# META           "id": "e0e2a368-38b1-41dc-8464-024db5db00d5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# <h1> Fred Silver Layer Transformations</h1>


# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import explode

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_mount_path(lakehouse_name):
    mnt_point = f'/mnt/mnt_{lakehouse_name}'
    mssparkutils.fs.mount(lakehouse_name, mnt_point)
    return f'file:{mssparkutils.fs.getMountPath(mnt_point)}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test = get_mount_path("Housing_Silver")
test

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

file_path = "Files/MORTGAGE30US_20250320175112.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read to Spark DataFrame 
df = spark.read.option("multiline", "true").json(file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

observations = df.select(explode('observations')).select(col("col.date").alias("Date"),col("col.value").alias("Interest Rate"),col("col.realtime_end").alias("Last Update Date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

observations.show(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode('append').saveAsTable('Avg 30 Year Mortgage Rates')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
