import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer trusted
customertrusted_node1758789627143 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-01/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1758789627143")

# Script generated for node accelrometer landing
accelrometerlanding_node1758789624963 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-01/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelrometerlanding_node1758789624963")

# Script generated for node Join
Join_node1758789741694 = Join.apply(frame1=customertrusted_node1758789627143, frame2=accelrometerlanding_node1758789624963, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1758789741694")

# Script generated for node SQL Query
SqlQuery3517 = '''
select user,timestamp,x,y,z from myDataSource

'''
SQLQuery_node1758789767741 = sparkSqlQuery(glueContext, query = SqlQuery3517, mapping = {"myDataSource":Join_node1758789741694}, transformation_ctx = "SQLQuery_node1758789767741")

# Script generated for node Drop Duplicates
DropDuplicates_node1758789807606 =  DynamicFrame.fromDF(SQLQuery_node1758789767741.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1758789807606")

# Script generated for node accelerometer trusted
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1758789807606, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758788997711", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrusted_node1758789821278 = glueContext.getSink(path="s3://stedi-human-balance-analytics-01/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrusted_node1758789821278")
accelerometertrusted_node1758789821278.setCatalogInfo(catalogDatabase="stedi-datalake",catalogTableName="accelerometer_trusted")
accelerometertrusted_node1758789821278.setFormat("json")
accelerometertrusted_node1758789821278.writeFrame(DropDuplicates_node1758789807606)
job.commit()
