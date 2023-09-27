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

# Script generated for node step_trainer_landing
step_trainer_landing_node1695850526925 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nd-spark-data-lake/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1695850526925",
)

# Script generated for node customer curated
customercurated_node1695850566277 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nd-spark-data-lake/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customercurated_node1695850566277",
)

# Script generated for node Join
Join_node1695850662826 = Join.apply(
    frame1=customercurated_node1695850566277,
    frame2=step_trainer_landing_node1695850526925,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1695850662826",
)

# Script generated for node Drop Fields
DropFields_node1695850968837 = DropFields.apply(
    frame=Join_node1695850662826,
    paths=[
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "`.serialNumber`",
    ],
    transformation_ctx="DropFields_node1695850968837",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1695850994387 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695850968837,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nd-spark-data-lake/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1695850994387",
)

job.commit()
