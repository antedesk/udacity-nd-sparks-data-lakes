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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nd-spark-data-lake/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1695666082028 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nd-spark-data-lake/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1695666082028",
)

# Script generated for node Join Step Trained and Accelerometer data
JoinStepTrainedandAccelerometerdata_node1695666090444 = Join.apply(
    frame1=StepTrainerTrusted_node1,
    frame2=AccelerometerTrusted_node1695666082028,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="JoinStepTrainedandAccelerometerdata_node1695666090444",
)

# Script generated for node Drop PII
DropPII_node1695666300416 = DropFields.apply(
    frame=JoinStepTrainedandAccelerometerdata_node1695666090444,
    paths=["user"],
    transformation_ctx="DropPII_node1695666300416",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropPII_node1695666300416,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nd-spark-data-lake/step_trainer/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node2",
)

job.commit()
