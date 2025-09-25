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
accelerometertrusted_node1758791398418 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-01/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometertrusted_node1758791398418")

# Script generated for node Step trainer trusted
Steptrainertrusted_node1758791399348 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-balance-analytics-01/step-trainer/trusted/"], "recurse": True}, transformation_ctx="Steptrainertrusted_node1758791399348")

# Script generated for node SQL Query
SqlQuery3570 = '''
select serialnumber,x,y,z,acc.timestamp from st join acc on
st.sensorreadingtime = acc.timestamp

'''
SQLQuery_node1758791501243 = sparkSqlQuery(glueContext, query = SqlQuery3570, mapping = {"st":Steptrainertrusted_node1758791399348, "acc":accelerometertrusted_node1758791398418}, transformation_ctx = "SQLQuery_node1758791501243")

# Script generated for node step trainer curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758791501243, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758788997711", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainercurated_node1758791699730 = glueContext.getSink(path="s3://stedi-human-balance-analytics-01/step-trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainercurated_node1758791699730")
steptrainercurated_node1758791699730.setCatalogInfo(catalogDatabase="stedi-datalake",catalogTableName="steptrainer_curated")
steptrainercurated_node1758791699730.setFormat("json")
steptrainercurated_node1758791699730.writeFrame(SQLQuery_node1758791501243)
job.commit()
