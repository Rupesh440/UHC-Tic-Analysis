from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, col, lit
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import monotonically_increasing_id
# Initialize Spark session
spark = SparkSession.builder \
    .appName("DwDesign") \
    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.3")\
    .getOrCreate()

# Define the path to the folder containing JSON files
json_folder_path = "/Volumes/RupeshSSD/Optum Tech Assesment/TiC_Analysis/downloaded_files/*.json"

# Read all JSON files in the folder
df = spark.read.option("multiline", "true").json(json_folder_path)

# Add a column with the input file name
#df_with_file_name = df.withColumn("input_file_name", input_file_name())

# Show the DataFrame with the file names
#df_with_file_name.show(truncate = False)


# Extract Reporting Entity Data
reporting_entity_df = df.select(
    "reporting_entity_name",
    "reporting_entity_type"
).distinct()
#df.printSchema()
#reporting_entity_df.show()
# Extract Plan Data
reporting_plans_df = df.select(explode_outer("reporting_structure").alias("reporting_structure"))\
                        .select(explode_outer("reporting_structure.reporting_plans").alias("plan"))

#reporting_plans_df.printSchema()
#reporting_plans_df.show()
plan_df = reporting_plans_df.select(
    col("plan.plan_id"),
    col("plan.plan_id_type"),
    col("plan.plan_market_type"),
    col("plan.plan_name")
).distinct()
#plan_df.show()
#plan_df.printSchema()

# Extract File Data
allowed_amount_df = df.select(explode_outer("reporting_structure").alias("reporting_structure"))\
    .select(lit("allowed_amount").alias("file_type"),
    col("reporting_structure.allowed_amount_file.description").alias("description"),
    col("reporting_structure.allowed_amount_file.location").alias("location")
).filter(col("description").isNotNull())

#allowed_amount_df.printSchema()
#allowed_amount_df.show()

in_network_files_df = df.select(explode_outer("reporting_structure").alias("reporting_structure"))\
                            .select(explode_outer("reporting_structure.in_network_files").alias("file"))
in_network_df = in_network_files_df.select(
    lit("in_network").alias("file_type"),
    col("file.description"),
    col("file.location")
).filter(col("description").isNotNull())
#in_network_df.printSchema()
#in_network_df.show()

file_df = allowed_amount_df.union(in_network_df).distinct()
#file_df.show()


# Generate IDs for Dimensions
reporting_entity_df = reporting_entity_df.withColumn("reporting_entity_id", monotonically_increasing_id())
plan_df = plan_df.withColumn("plan_unique_id", monotonically_increasing_id())
file_df = file_df.withColumn("file_id", monotonically_increasing_id())
#reporting_entity_df.show()
#plan_df.show()
#file_df.show()

# Create temporary views of dimension tables for joining
reporting_entity_df.createOrReplaceTempView("dim_reporting_entity")
plan_df.createOrReplaceTempView("dim_plan")
file_df.createOrReplaceTempView("dim_file")

# Explode the nested structures
exploded_df = (df.withColumn("reporting_structure",explode_outer("reporting_structure"))
                    .withColumn("reporting_plans", explode_outer("reporting_structure.reporting_plans")) \
                       .withColumn("in_network_files", explode_outer("reporting_structure.in_network_files")))

#exploded_df.show()

# Extract required data for fact table
fact_df = exploded_df.select(
    col("reporting_entity_name"),
    col("reporting_entity_type"),
    col("reporting_plans.plan_id").alias("plan_id"),
    col("reporting_plans.plan_id_type").alias("plan_id_type"),
    col("reporting_plans.plan_market_type").alias("plan_market_type"),
    col("reporting_plans.plan_name").alias("plan_name"),
    lit("in_network").alias("file_type"),
    col("in_network_files.description").alias("file_description"),
    col("in_network_files.location").alias("file_location")
)
#fact_df.show()

# Create a temporary view for joining
fact_df.createOrReplaceTempView("source")

# Join with dimensions
fact_reporting_df = spark.sql("""
    SELECT 
        dre.reporting_entity_id,
        dp.plan_id,
        df.file_id,
        s.reporting_entity_name,
        s.reporting_entity_type,
        s.plan_id,
        s.plan_id_type,
        s.plan_market_type,
        s.plan_name,
        s.file_type,
        s.file_description,
        s.file_location
    FROM source s
    JOIN dim_reporting_entity dre ON s.reporting_entity_name = dre.reporting_entity_name
    JOIN dim_plan dp ON s.plan_id = dp.plan_id
    JOIN dim_file df ON s.file_description = df.description AND s.file_type = df.file_type
""")

#fact_reporting_df.show()


# Write DataFrames to Snowflake
snowflake_options = {
    "sfURL": "https://gs50388.north-europe.azure.snowflakecomputing.com",
    "sfAccount": "gs50388.north-europe",
    "sfUser": "RUPESH",
    "sfPassword": "Rupesh4005@",
    "sfDatabase": "UHC_TIC",
    "sfSchema": "TIC_WAREHOUSE",
    "sfWarehouse": "COMPUTE_WH"
}


reporting_entity_df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "dim_reporting_entity") \
    .mode("overwrite") \
    .save()

fact_df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "fact_df") \
    .mode("overwrite") \
    .save()


file_df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "dim_file") \
    .mode("overwrite") \
    .save()

plan_df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "dim_plan") \
    .mode("overwrite") \
    .save()

# Stop Spark session
spark.stop()
