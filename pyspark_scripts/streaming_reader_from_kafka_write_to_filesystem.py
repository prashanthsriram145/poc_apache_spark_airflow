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
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/learning") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/learning") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()

    df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:29092') \
        .option("subscribe", "inbound-data") \
        .load()

    df1 = df.selectExpr("CAST(value as STRING) as json")

    df2 = df1.select(from_json(col("json"), schema=schema).alias("json")).select("json.*")

    df2.writeStream.format("json") \
        .outputMode("append") \
        .option('checkpointLocation', '../checkpoint') \
        .option('path', "../output") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    streaming_read_from_kafka()
