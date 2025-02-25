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
AmazonS3_node1740214507163 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://ads507-footballapi/fixture_statistics.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1740214507163")

# Script generated for node SQL Query
SqlQuery5568 = '''
SELECT fixture, team_name, stat_type, stat_value
FROM (
    SELECT 
        fixture,
        `team.name` AS team_name,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[0].type'
        ) AS stat_type,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[0].value'
        ) AS stat_value
    FROM fixture_statistics

    UNION ALL

    SELECT 
        fixture,
        `team.name` AS team_name,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[1].type'
        ) AS stat_type,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[1].value'
        ) AS stat_value
    FROM fixture_statistics

    UNION ALL

    SELECT 
        fixture,
        `team.name` AS team_name,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[2].type'
        ) AS stat_type,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[2].value'
        ) AS stat_value
    FROM fixture_statistics

    UNION ALL

    SELECT 
        fixture,
        `team.name` AS team_name,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[3].type'
        ) AS stat_type,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[3].value'
        ) AS stat_value
    FROM fixture_statistics

    UNION ALL

    SELECT 
        fixture,
        `team.name` AS team_name,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[4].type'
        ) AS stat_type,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[4].value'
        ) AS stat_value
    FROM fixture_statistics

    UNION ALL

    SELECT 
        fixture,
        `team.name` AS team_name,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[5].type'
        ) AS stat_type,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[5].value'
        ) AS stat_value
    FROM fixture_statistics

    UNION ALL

    SELECT 
        fixture,
        `team.name` AS team_name,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[6].type'
        ) AS stat_type,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[6].value'
        ) AS stat_value
    FROM fixture_statistics

    UNION ALL

    SELECT 
        fixture,
        `team.name` AS team_name,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[7].type'
        ) AS stat_type,
        get_json_object(
            REGEXP_REPLACE(statistics, "[']", "\\\""), 
            '$[7].value'
        ) AS stat_value
    FROM fixture_statistics
) AS combined_types
WHERE stat_type IS NOT NULL;
'''
SQLQuery_node1740216962495 = sparkSqlQuery(glueContext, query = SqlQuery5568, mapping = {"fixture_statistics":AmazonS3_node1740214507163}, transformation_ctx = "SQLQuery_node1740216962495")

# Script generated for node Amazon Redshift
AmazonRedshift_node1740217636763 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1740216962495, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-248189906309-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.statistics", "connectionName": "glue_football", "preactions": "CREATE TABLE IF NOT EXISTS public.statistics (fixture VARCHAR, team_name VARCHAR, stat_type VARCHAR, stat_value VARCHAR);"}, transformation_ctx="AmazonRedshift_node1740217636763")

job.commit()