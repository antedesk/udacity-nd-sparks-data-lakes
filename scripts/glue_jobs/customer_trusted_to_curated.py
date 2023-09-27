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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nd-spark-data-lake/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1695163497282 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://nd-spark-data-lake/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1695163497282",
)

# Script generated for node Join Customer
JoinCustomer_node1695163712195 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerTrustedZone_node1695163497282,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1695163712195",
)

# Script generated for node Drop Fields
DropFields_node1695164087048 = DropFields.apply(
    frame=JoinCustomer_node1695163712195,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1695164087048",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695164087048,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://nd-spark-data-lake/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
