import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1740370493517 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://ads507-footballapi/fixtures.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1740370493517")

# Script generated for node SQL Query
SqlQuery3167 = '''
SELECT 
    `fixture.id` AS fixture_id,
    CAST(`fixture.date` AS TIMESTAMP) AS fixture_date,
    `fixture.venue.id` AS fixture_venue_id,
    `fixture.venue.name` AS fixture_venue_name,
    `fixture.venue.city` AS fixture_venue_city,
    `goals.home` AS goals_home,
    `goals.away` AS goals_away,
    `teams.home.id` as home_team_id,
    `teams.home.name` as home_team_name,
    `teams.away.id` as away_team_id,
    `teams.away.name` as away_team_name
FROM myDataSource
'''
SQLQuery_node1740370512308 = sparkSqlQuery(glueContext, query = SqlQuery3167, mapping = {"myDataSource":AmazonS3_node1740370493517}, transformation_ctx = "SQLQuery_node1740370512308")

# Script generated for node Amazon Redshift
AmazonRedshift_node1740370517910 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1740370512308, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-248189906309-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.fixtures", "connectionName": "glue_football", "preactions": "CREATE TABLE IF NOT EXISTS public.fixtures (fixture_id VARCHAR, fixture_date TIMESTAMP, fixture_venue_id VARCHAR, fixture_venue_name VARCHAR, fixture_venue_city VARCHAR, goals_home VARCHAR, goals_away VARCHAR, home_team_id VARCHAR, home_team_name VARCHAR, away_team_id VARCHAR, away_team_name VARCHAR);"}, transformation_ctx="AmazonRedshift_node1740370517910")

job.commit()