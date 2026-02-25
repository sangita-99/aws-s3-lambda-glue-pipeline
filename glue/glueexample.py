import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Receive arguments passed from Lambda
args = getResolvedOptions(sys.argv, ["JOB_NAME", "VAL1", "VAL2"])

val1 = args["VAL1"]  # file_name
val2 = args["VAL2"]  # bucket_name

print("VAL1 (file_name):", val1)
print("VAL2 (bucket_name):", val2)

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read CSV from source bucket
src_file = f"s3://{val2}/{val1}"
df = spark.read.option("header", "true").csv(src_file)
print("Read successful")

# Write to destination bucket
dest = f"s3://destination-takeo/{val1}"
df.write.mode("overwrite").option("header", "true").csv(dest)
print("Write successful")

# Commit Glue job
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
job.commit()