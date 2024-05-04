from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType, DoubleType
from pyspark.sql.functions import from_json, col, explode
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

    weather_schema = StructType([
        StructField("coord", StructType([
            StructField("lon", FloatType()),
            StructField("lat", FloatType())
        ])),
        StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType()),
        StructField("main", StringType()),
        StructField("description", StringType()),
        StructField("icon", StringType())
    ]))),
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

    air_quality_schema = StructType([
        StructField("status", StringType(), True),
        StructField("data", StructType([
            StructField("aqi", IntegerType(), True),
            StructField("idx", IntegerType(), True),
            StructField("attributions", ArrayType(StructType([
                StructField("url", StringType(), True),
                StructField("name", StringType(), True),
                StructField("logo", StringType(), True)
            ])), True),
            StructField("city", StructType([
                StructField("geo", ArrayType(DoubleType()), True),
                StructField("name", StringType(), True),
                StructField("url", StringType(), True),
                StructField("location", StringType(), True)
            ]), True),
            StructField("dominentpol", StringType(), True),
            StructField("iaqi", StructType([
                StructField("dew", StructType([StructField("v", DoubleType(), True)]), True),
                StructField("h", StructType([StructField("v", DoubleType(), True)]), True),
                StructField("no2", StructType([StructField("v", DoubleType(), True)]), True),
                StructField("o3", StructType([StructField("v", DoubleType(), True)]), True),
                StructField("p", StructType([StructField("v", DoubleType(), True)]), True),
                StructField("pm10", StructType([StructField("v", DoubleType(), True)]), True),
                StructField("pm25", StructType([StructField("v", DoubleType(), True)]), True),
                StructField("t", StructType([StructField("v", DoubleType(), True)]), True),
                StructField("w", StructType([StructField("v", DoubleType(), True)]), True),
                StructField("wg", StructType([StructField("v", DoubleType(), True)]), True)
            ]), True),
            StructField("time", StructType([
                StructField("s", TimestampType(), True),
                StructField("tz", StringType(), True),
                StructField("v", IntegerType(), True),
                StructField("iso", StringType(), True)
            ]), True),
            StructField("forecast", StructType([
                StructField("daily", StructType([
                    StructField("o3", ArrayType(StructType([
                        StructField("avg", IntegerType(), True),
                        StructField("day", StringType(), True),
                        StructField("max", IntegerType(), True),
                        StructField("min", IntegerType(), True)
                    ])), True),
                    StructField("pm10", ArrayType(StructType([
                        StructField("avg", IntegerType(), True),
                        StructField("day", StringType(), True),
                        StructField("max", IntegerType(), True),
                        StructField("min", IntegerType(), True)
                    ])), True),
                    StructField("pm25", ArrayType(StructType([
                        StructField("avg", IntegerType(), True),
                        StructField("day", StringType(), True),
                        StructField("max", IntegerType(), True),
                        StructField("min", IntegerType(), True)
                    ])), True)
                ]), True)
            ]), True),
            StructField("debug", StructType([
                StructField("sync", StringType(), True)
            ]), True)
        ]), True)
    ])


    def read_kafka_weather_topic(topic, schema):
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
            .selectExpr(
                'coord.lon AS longitude',
                'coord.lat AS latitude',
                'weather[0].id AS weather_id',
                'weather[0].main AS weather_main',
                'weather[0].description AS weather_description',
                'weather[0].icon AS weather_icon',
                'base',
                'main.temp AS main_temp',
                'main.feels_like AS main_feels_like',
                'main.temp_min AS main_temp_min',
                'main.temp_max AS main_temp_max',
                'main.pressure AS main_pressure',
                'main.humidity AS main_humidity',
                'visibility',
                'wind.speed AS wind_speed',
                'wind.deg AS wind_deg',
                'clouds.all AS clouds',
                'dt',
                'sys.type AS sys_type',
                'sys.id AS sys_id',
                'sys.country AS sys_country',
                'sys.sunrise AS sys_sunrise',
                'sys.sunset AS sys_sunset',
                'timezone',
                'id',
                'name',
                'cod'
            )
        )

    def read_kafka_air_quality_topic(topic, schema):
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
        .selectExpr(
                "data.city.name AS city_name",
                "data.city.geo AS city_geo",
                "data.aqi AS aqi",
                "data.dominentpol",
                "data.iaqi.o3.v AS o3",
                "data.iaqi.pm10.v AS pm10",
                "data.iaqi.pm25.v AS pm25",
                "data.time.s AS time_s"
            )
    )


    def streamWriter(input, checkpointFolder, output):  
            return (input.writeStream
                    .format('parquet')
                    .option('checkpointLocation', checkpointFolder)
                    .option('failOnDataLoss', 'false')
                    .option('path', output)
                    .outputMode('append')
                    .start())

    weather_dataDF = read_kafka_weather_topic('weather_data', weather_schema).alias('weather')
    air_quality_dataDF = read_kafka_air_quality_topic('air_quality_data', air_quality_schema).alias('air_quality')

    query1 = streamWriter(weather_dataDF, 's3a://olivier-spark-streaming-data/checkpoints/weather_data',
                         's3a://olivier-spark-streaming-data/data/weather_data')
    
    query2 = streamWriter(air_quality_dataDF, 's3a://olivier-spark-streaming-data/checkpoints/air_quality_data',
                         's3a://olivier-spark-streaming-data/data/air_quality_data')

    query2.awaitTermination()

if __name__ == "__main__":
    main()
