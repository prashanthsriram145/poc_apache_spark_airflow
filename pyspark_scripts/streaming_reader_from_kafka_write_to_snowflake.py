from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import from_json, col


def streaming_read_from_kafka():
    structFields = list()
    structFields.append(StructField('firstName', StringType(), True))
    structFields.append(StructField('lastName', StringType(), True))
    structFields.append(StructField('age', IntegerType(), True))
    schema = StructType(structFields)

    spark = SparkSession.builder.appName('streaming_reader_from_kafka').master("local") \
        .getOrCreate()

    sfOptions = {
        "sfUrl": "HE94383.snowflakecomputing.com",
        "sfUser": "prahanthsriram145",
        "sfPassword": "Snowden@145",
        "sfDatabase": "sample",
        "sfSchema": "public",
        "sfWarehouse": "compute_wh"
    }

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
        .option("sfUrl", "HE94383.snowflakecomputing.com") \
        .option("sfUser", "prahanthsriram145") \
        .option("sfPassword", "Snowden@145") \
        .option("sfDatabase", "sample") \
        .option("sfSchema", "public") \
        .option("sfWarehouse", "compute_wh") \
        .option("dbtable", "emp_basic") \
        .load()

    df.show()

    # df = spark.readStream \
    #     .format('kafka') \
    #     .option('kafka.bootstrap.servers', 'localhost:29092') \
    #     .option("subscribe", "inbound") \
    #     .load()
    #
    # df1 = df.selectExpr("CAST(value as STRING) as json")
    #
    # df2 = df1.select(from_json(col("json"), schema=schema).alias("json")).select("json.*")

    # df2.writeStream \
    #     .format("mongodb") \
    #     .option('checkpointLocation', '../checkpoint') \
    #     .option("spark.mongodb.connection.uri", "mongodb://127.0.0.1:27017/") \
    #     .option("spark.mongodb.change.stream.publish.full.document.only", "true") \
    #     .option("spark.mongodb.database", "learning") \
    #     .option("spark.mongodb.collection", "persons") \
    #     .outputMode("append") \
    #     .start() \
    #     .awaitTermination()


if __name__ == '__main__':
    streaming_read_from_kafka()
