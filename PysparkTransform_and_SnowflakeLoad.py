from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, col, lit
from pyspark.sql.functions import input_file_name
# Initialize Spark session
spark = SparkSession.builder \
    .appName("TransformLoad") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.3")\
    .getOrCreate()

# Define the path to the folder containing JSON files
json_folder_path = "/Volumes/RupeshSSD/Optum Tech Assesment/TiC_Analysis/downloaded_files/*.json"

# Read all JSON files in the folder
df = spark.read.option("multiline", "true").json(json_folder_path)

# Add a column with the input file name
df_with_file_name = df.withColumn("input_file_name", input_file_name())

# Show the DataFrame with the file names
#df_with_file_name.show(truncate = False)

# Flatten the DataFrame
df = df.select(
    "reporting_entity_name",
    "reporting_entity_type",
    explode_outer("reporting_structure").alias("reporting_structure")
)

df.printSchema()

# Extract reporting plans
reporting_plans = df.withColumn("reporting_plans", explode_outer(df.reporting_structure.reporting_plans)) \
                             .select(
                                 "reporting_entity_name",
                                 "reporting_entity_type",
                                 "reporting_plans.plan_name",
                                 "reporting_plans.plan_id",
                                 "reporting_plans.plan_id_type",
                                 "reporting_plans.plan_market_type"
                             )

# Show the Reporting Plans DataFrame
reporting_plans.show()

reporting_plans.printSchema()

# Extract in-network files
in_network_files = df.withColumn("reporting_plans", explode_outer(df.reporting_structure.reporting_plans)) \
                              .withColumn("in_network_files", explode_outer(df.reporting_structure.in_network_files)) \
                              .select(lit("in_network").alias("file_type"),
                                  "reporting_entity_name",
                                  "reporting_entity_type",
                                  "reporting_plans.plan_name",
                                  "reporting_plans.plan_id",
                                  "in_network_files.description",
                                  "in_network_files.location"
                              )

# Show the In-Network Files DataFrame
in_network_files.show()

in_network_files.printSchema()

# Extract in-network files
allowed_amount_files = df.withColumn("reporting_plans", explode_outer(df.reporting_structure.reporting_plans))\
                              .select(lit("allowed_amount").alias("file_type"),
                                  "reporting_entity_name",
                                  "reporting_entity_type",
                                  "reporting_plans.plan_name",
                                  "reporting_plans.plan_id",
                                  "reporting_structure.allowed_amount_file.description",
                                  "reporting_structure.allowed_amount_file.location"
                              )

# Show the In-Network Files DataFrame
allowed_amount_files.show()

allowed_amount_files.printSchema()

file_df = in_network_files.union(allowed_amount_files).distinct()

# Write DataFrames to Snowflake
#Please reach out to me for the credentials
snowflake_options = {
    "sfURL": "https://gs50388.north-europe.azure.snowflakecomputing.com",
    "sfAccount": "gs50388.north-europe",
    "sfUser": "*****",
    "sfPassword": "******",
    "sfDatabase": "*****",
    "sfSchema": "*****",
    "sfWarehouse": "*****"
}


reporting_plans.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "reporting_plans") \
    .mode("overwrite") \
    .save()

in_network_files.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "in_network_files") \
    .mode("overwrite") \
    .save()

allowed_amount_files.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "allowed_amount_files") \
    .mode("overwrite") \
    .save()

file_df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "file_df_combined") \
    .mode("overwrite") \
    .save()
# Stop Spark session
spark.stop()