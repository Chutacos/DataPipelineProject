import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, explode, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# Initialize Glue Context and Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load the new file from S3
AmazonS3_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"", 
        "withHeader": True, 
        "separator": ",", 
        "optimizePerformance": False
    }, 
    connection_type="s3", 
    format="csv", 
    connection_options={
        "paths": ["s3://ads507-footballapi/fixture_lineups.csv"], 
        "recurse": True
    }, 
    transformation_ctx="AmazonS3_node"
)

# Convert DynamicFrame to DataFrame for transformations
df = AmazonS3_node.toDF()

# Define Schema for JSON Fields
player_schema = StructType([
    StructField("player", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("pos", StringType(), True)
    ]))
])

# Parse JSON Strings into Array of Structs
df = df.withColumn("startXI", from_json(col("startXI"), ArrayType(player_schema))) \
       .withColumn("substitutes", from_json(col("substitutes"), ArrayType(player_schema))) \
       .withColumnRenamed("fixture", "match_id") \
       .withColumnRenamed("team.id", "team_id") \
       .withColumnRenamed("team.name", "team_name") \
       .withColumn("match_id", col("match_id").cast(IntegerType())) \
       .withColumn("team_logo", col("`team.logo`")) \
       .withColumn("coach_id", col("`coach.id`").cast(IntegerType())) \
       .withColumn("coach_name", col("`coach.name`")) \
       .withColumn("coach_photo", col("`coach.photo`"))

# Explode the startXI array to flatten the data for starters
df_starters = df.select(
    col("formation"),
    explode(col("startXI")).alias("player_info"),
    col("team_id"),
    col("team_name"),
    col("match_id"),
    col("team_logo"),
    col("coach_id"),
    col("coach_name"),
    col("coach_photo")
).select(
    col("formation"),
    col("player_info.player.id").alias("player_id"),
    col("player_info.player.name").alias("player_name"),
    col("player_info.player.number").alias("player_number"),
    col("player_info.player.pos").alias("player_position"),
    lit("starter").alias("role"),
    col("team_id"),
    col("team_name"),
    col("match_id"),
    col("team_logo"),
    col("coach_id"),
    col("coach_name"),
    col("coach_photo")
)

# Explode the substitutes array to flatten the data for substitutes
df_substitutes = df.select(
    col("formation"),
    explode(col("substitutes")).alias("player_info"),
    col("team_id"),
    col("team_name"),
    col("match_id"),
    col("team_logo"),
    col("coach_id"),
    col("coach_name"),
    col("coach_photo")
).select(
    col("formation"),
    col("player_info.player.id").alias("player_id"),
    col("player_info.player.name").alias("player_name"),
    col("player_info.player.number").alias("player_number"),
    col("player_info.player.pos").alias("player_position"),
    lit("substitute").alias("role"),
    col("team_id"),
    col("team_name"),
    col("match_id"),
    col("team_logo"),
    col("coach_id"),
    col("coach_name"),
    col("coach_photo")
)

# Union starters and substitutes dataframes
df_combined = df_starters.unionByName(df_substitutes)

# Convert back to DynamicFrame for Glue compatibility
dynamic_frame_combined = DynamicFrame.fromDF(df_combined, glueContext, "dynamic_frame_combined")

# Write to Redshift
Redshift_node = glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_combined, 
    connection_type="redshift", 
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-248189906309-us-east-1/temporary/", 
        "useConnectionProperties": "true", 
        "dbtable": "public.lineups_updated", 
        "connectionName": "glue_football",
        "preactions": """
            TRUNCATE TABLE public.lineups_updated;
            CREATE TABLE IF NOT EXISTS public.lineups_updated (
                match_id INT,
                formation VARCHAR, 
                player_id INT, 
                player_name VARCHAR, 
                player_number INT, 
                player_position VARCHAR, 
                role VARCHAR, 
                team_id INT, 
                team_name VARCHAR,
                team_logo VARCHAR,
                coach_id INT,
                coach_name VARCHAR,
                coach_photo VARCHAR
            );
        """
    }, 
    transformation_ctx="Redshift_node"
)

# Commit the Glue job
job.commit()
