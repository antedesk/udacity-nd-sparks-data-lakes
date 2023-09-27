import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1695836027316 = glueContext.create_dynamic_frame.from_catalog(
    database="nd-spark-datalake",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1695836027316",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1695836070162 = glueContext.create_dynamic_frame.from_catalog(
    database="nd-spark-datalake",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1695836070162",
)

# Script generated for node Join
Join_node1695836110841 = Join.apply(
    frame1=AccelerometerTrusted_node1695836027316,
    frame2=StepTrainerTrusted_node1695836070162,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1695836110841",
)

# Script generated for node Drop PII Fields
DropPIIFields_node1695836148462 = DropFields.apply(
    frame=Join_node1695836110841,
    paths=["user"],
    transformation_ctx="DropPIIFields_node1695836148462",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1695836171607 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropPIIFields_node1695836148462,
    database="nd-spark-datalake",
    table_name="machine_learning_curated",
    transformation_ctx="MachineLearningCurated_node1695836171607",
)

job.commit()
