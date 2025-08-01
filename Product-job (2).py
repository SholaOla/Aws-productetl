import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Amazon S3
AmazonS3_node1753496902565 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://moni-landing01/raw/raw_products.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1753496902565")

# Script generated for node 
_node1753497327053 = DropFields.apply(frame=AmazonS3_node1753496902565, paths=[], transformation_ctx="_node1753497327053")

# Script generated for node Drop Duplicates
DropDuplicates_node1753497587312 =  DynamicFrame.fromDF(_node1753497327053.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1753497587312")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1753497587312, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1753496541483", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1753497764509 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1753497587312, connection_type="s3", format="glueparquet", connection_options={"path": "s3://tranformed-product", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1753497764509")

job.commit()