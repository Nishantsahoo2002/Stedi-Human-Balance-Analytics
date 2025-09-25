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

# Script generated for node customer curated
customercurated_node1758790728404 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-01/customer/curated/"], "recurse": True}, transformation_ctx="customercurated_node1758790728404")

# Script generated for node step trainer landing
steptrainerlanding_node1758790736440 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-01/step-trainer/landing/"], "recurse": True}, transformation_ctx="steptrainerlanding_node1758790736440")

# Script generated for node SQL Query
SqlQuery3570 = '''
select s.* from s join c on
c.serialnumber=s.serialnumber

'''
SQLQuery_node1758790818927 = sparkSqlQuery(glueContext, query = SqlQuery3570, mapping = {"c":customercurated_node1758790728404, "s":steptrainerlanding_node1758790736440}, transformation_ctx = "SQLQuery_node1758790818927")

# Script generated for node step trainer trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758790818927, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758788997711", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrusted_node1758790893248 = glueContext.getSink(path="s3://stedi-human-balance-analytics-01/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrusted_node1758790893248")
steptrainertrusted_node1758790893248.setCatalogInfo(catalogDatabase="stedi-datalake",catalogTableName="steptrainer_trusted")
steptrainertrusted_node1758790893248.setFormat("json")
steptrainertrusted_node1758790893248.writeFrame(SQLQuery_node1758790818927)
job.commit()
