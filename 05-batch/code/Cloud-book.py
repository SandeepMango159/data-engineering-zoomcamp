# %% [markdown]
# # This notebook is for creating a master spark cluster and connecting to GCS

# %% [markdown]
# ## Setup spark

# %% [markdown]
# ### Setup python environments

# %%
# Add the vevn to spark's settings, so inject the venv’s Python into both driver & worker configs before recreating the session, to find the right python interpreter, since the notebook us running within the venv kernel, we can just use sys.executable
import os, sys
import argparse

#couple of command line arguments
parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


print(sys.executable)
venv_python = sys.executable

# 1) Ensure the worker uses exactly this Python executable:
os.environ['PYSPARK_PYTHON'] = venv_python
os.environ['PYSPARK_DRIVER_PYTHON'] = venv_python

print(os.environ['PYSPARK_PYTHON'])
print(os.environ['PYSPARK_DRIVER_PYTHON'])


# %% [markdown]
# ### Launch spark with those python variables

# %%
import pyspark
from pyspark.sql import SparkSession

# We do not want to specify master here, especially if it's running in a cloud environment like dataproc, that way dataproc will correctly assign the master itself
spark = SparkSession.builder \
    .appName('test') \
    .config("spark.pyspark.python", venv_python) \
    .config("spark.pyspark.driver.python", venv_python) \
    .getOrCreate()

# %%
# Check which port the Spark UI is running on
print(spark.sparkContext.uiWebUrl)

# %%
# Print spark to show all the spark info
spark

# %% [markdown]
# ## Testing the cluster by reading files and writing, same as book 6

# %% [markdown]
# ### Read green files

# %%
# Use 2 * since we have year subfolders, and month subfolders, okay...
df_green = spark.read.parquet(input_green)

# %%
# After reading the green parquet files, we show them
df_green.show()

# %%


# %%
# Check the schemas, make sure it's the same as the one that we wrote to the parquet file
df_green.schema

# %%
df_green.printSchema()

# %%
# Rename columns to make it easier to read
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

# %%
# Show to make sure columns got renamed
df_green.printSchema()

# %% [markdown]
# ### Read yellow files

# %%
df_yellow = spark.read.parquet(input_yellow)

# %%
# After reading the green parquet files, we show them
df_yellow.show()

# %%
# count just to make sure it's right
df_yellow.count()

# %%
# Check the schemas, make sure it's the same as the one that we wrote to the parquet file
df_yellow.schema

# %%
df_yellow.printSchema()

# %%
# Rename columns to make it easier to read, and also join with the green schema data
df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

# %%
# Show to make sure columns got renamed
df_yellow.printSchema()

# %% [markdown]
# ### Joining both dataframes together

# %% [markdown]
# #### Each dataframe has a columns field, which we can use to get the columns that both dataframes have in common, meaning which ones exist in both dataframes
# ##### We have already renamed the pickup and dropoffdatetime columns for both dataframes

# %%
df_green.columns

# %%
# So we'll write a small function for that, that holds a common columns list
# Puts the columns of yellow into a list
# And iterate over the green columns, if they're also in the yellow columns set, we'll add them to the list
# This way preserves the order of columns as defined by the spark dataframe when reading the parquet files
common_colums = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_colums.append(col)

# %%
common_colums = [
'VendorID',
 'pickup_datetime',
 'dropoff_datetime',
 'store_and_fwd_flag',
 'RatecodeID',
 'PULocationID',
 'DOLocationID',
 'passenger_count',
 'trip_distance',
 'fare_amount',
 'extra',
 'mta_tax',
 'tip_amount',
 'tolls_amount',
 'improvement_surcharge',
 'total_amount',
 'payment_type',
 'congestion_surcharge']

# %%
from pyspark.sql import functions as F

# %%
# Using select we can select the list of common columns, 
# We'll also add a service type to distinguish where they come from
# F.lit for literal
df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))

# Print to make sure it works
df_green_sel

# %%
# Same for this yellow dataframe
df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))

df_yellow_sel

# %%
# Join them together
df_trips_data = df_green_sel.unionAll(df_yellow_sel)

# %%
# Check all the columns again
df_trips_data.columns

# %%
# Count the rows by grouping by service type to make sure they're the same as before 
df_trips_data.groupBy('service_type').count().show()

# %% [markdown]
# ### Writing the SQL code for spark dataframes

# %% [markdown]
# ##### First we have to be able to use the dataframe, we do that by registering it as a table, otherwise SQL statements won't work on it

# %%
# We create a table for it, but this method is deprecated so use the new method below
# df_trips_data.registerTempTable('trips_data')

# %%
# The new suggested method to use instead
df_trips_data.createOrReplaceTempView('trips_data')

# %%
# Now we can write sql and access that dataframe by the table name, results are the same as above
spark.sql("""
SELECT
    service_type,
    count(1)
FROM
    trips_data
GROUP BY 
    service_type
""").show()

# %%
# Execute the same query from week 4, for dm_monthly_zone_revenue

df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

# %%
# Show the result to check it outselves...
df_result.show()

# %%
# Count to see how many rows we have, must have a row for each locationid, month and service type combination
df_result.count()

# %%
# And write out the fie
# Use coalesce, it's like partition, but to reduce partitions, so it'll reduce it to 1 partition
df_result.coalesce(1).write.parquet(output, mode='overwrite')


