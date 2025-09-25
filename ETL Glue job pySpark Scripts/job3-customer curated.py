import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer trusted
accelerometertrusted_node1758790113625 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-01/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometertrusted_node1758790113625")

# Script generated for node customer trusted
customertrusted_node1758790112594 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-01/customer/trusted/"], "recurse": True}, transformation_ctx="customertrusted_node1758790112594")

# Script generated for node SQL Query
SqlQuery3503 = '''
select distinct ct.* from ct join act on
act.user = ct.email

'''
SQLQuery_node1758790195153 = sparkSqlQuery(glueContext, query = SqlQuery3503, mapping = {"act":accelerometertrusted_node1758790113625, "ct":customertrusted_node1758790112594}, transformation_ctx = "SQLQuery_node1758790195153")

# Script generated for node customer curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758790195153, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758788997711", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customercurated_node1758790291788 = glueContext.getSink(path="s3://stedi-human-balance-analytics-01/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customercurated_node1758790291788")
customercurated_node1758790291788.setCatalogInfo(catalogDatabase="stedi-datalake",catalogTableName="customer_curated")
customercurated_node1758790291788.setFormat("json")
customercurated_node1758790291788.writeFrame(SQLQuery_node1758790195153)
job.commit()
