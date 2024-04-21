from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import from_json, col
from config import configuration

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Kafka to Parquet") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # Define schema for JSON data
    schema = StructType([
        StructField("coord", StructType([
            StructField("lon", FloatType()),
            StructField("lat", FloatType())
        ])),
        StructField("weather", StructType([
            StructField("id", IntegerType()),
            StructField("main", StringType()),
            StructField("description", StringType()),
            StructField("icon", StringType())
        ])),
        StructField("base", StringType()),
        StructField("main", StructType([
            StructField("temp", FloatType()),
            StructField("feels_like", FloatType()),
            StructField("temp_min", FloatType()),
            StructField("temp_max", FloatType()),
            StructField("pressure", IntegerType()),
            StructField("humidity", IntegerType())
        ])),
        StructField("visibility", IntegerType()),
        StructField("wind", StructType([
            StructField("speed", FloatType()),
            StructField("deg", IntegerType())
        ])),
        StructField("clouds", StructType([
            StructField("all", IntegerType())
        ])),
        StructField("dt", TimestampType()), 
        StructField("sys", StructType([
            StructField("type", IntegerType()),
            StructField("id", IntegerType()),
            StructField("country", StringType()),
            StructField("sunrise", IntegerType()),
            StructField("sunset", IntegerType())
        ])),
        StructField("timezone", IntegerType()),
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("cod", IntegerType())
    ])

    
    def read_kafka_topic(topic, schema):
        return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'broker:29092')
            .option('subscribe', topic)
            .option('startingOffsets', 'earliest')
            .option('failOnDataLoss', 'false') 
            .load()
            .selectExpr('CAST(value AS STRING)')
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('dt', '2 minutes')
            )

    
    def streamWriter(input, checkpointFolder, output):  # Remove DataFrame type hint
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('failOnDataLoss', 'false')
                .option('path', output)
                .outputMode('append')
                .start())

    dataDF = read_kafka_topic('weather_data', schema).alias('weather')

    query = streamWriter(dataDF, 's3a://olivier-spark-streaming-data/checkpoints/weather_data',
                         's3a://olivier-spark-streaming-data/data/weather_data')

    query.awaitTermination()

if __name__ == "__main__":
    main()
