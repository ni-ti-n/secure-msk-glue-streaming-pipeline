import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME","database_name","table_name","topic_name","dest_dir"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Kafka Stream
dataframe_KafkaStream_node1 = glueContext.create_data_frame.from_catalog(
    database = args['database_name'],
    table_name = args['table_name'],
    additional_options={"startingOffsets": "earliest", "inferSchema": "false"},
    transformation_ctx="dataframe_KafkaStream_node1",
)


def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        KafkaStream_node1 = DynamicFrame.fromDF(
            data_frame, glueContext, "from_data_frame"
        )
        ApplyMapping_node2 = ApplyMapping.apply(
            frame=KafkaStream_node1,
            mappings=[
                ("year", "long", "year", "long"),
                ("month", "long", "month", "long"),
                ("day", "long", "day", "long"),
                ("dep_time", "long", "dep_time", "long"),
                ("dep_delay", "long", "dep_delay", "long"),
                ("arr_time", "long", "arr_time", "long"),
                ("arr_delay", "long", "arr_delay", "long"),
                ("carrier", "string", "carrier", "string"),
                ("tailnum", "string", "tailnum", "string"),
                ("flight", "long", "flight", "long"),
                ("origin", "string", "origin", "string"),
                ("dest", "string", "dest", "string"),
                ("air_time", "long", "air_time", "long"),
                ("distance", "long", "distance", "long"),
                ("hour", "long", "hour", "long"),
                ("minute", "long", "minute", "long"),
            ],
            transformation_ctx="ApplyMapping_node2",
        )

        # Script generated for node S3 bucket
        S3bucket_node3_path = (
            args['dest_dir']
            + "output"
            + "/"
        )
        S3bucket_node3 = glueContext.getSink(
            path=S3bucket_node3_path,
            connection_type="s3",
            updateBehavior="UPDATE_IN_DATABASE",
            partitionKeys=["year", "month", "day"],
            enableUpdateCatalog=True,
            transformation_ctx="S3bucket_node3",
        )
        S3bucket_node3.setCatalogInfo(catalogDatabase="glue_kafka_blog_db", catalogTableName="output")
        S3bucket_node3.setFormat("glueparquet")
        S3bucket_node3.writeFrame(ApplyMapping_node2)


glueContext.forEachBatch(
    frame=dataframe_KafkaStream_node1,
    batch_function=processBatch,
    options={
        "windowSize": "30 seconds",
        "checkpointLocation": args["dest_dir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)