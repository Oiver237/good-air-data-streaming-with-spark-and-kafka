# good-air-data-streaming-with-spark-and-kafka

This Spark Streaming pipeline aims to ingest weather data and air quality data of the 10 biggest cities in France from Apache Kafka topics, process it using Spark Structured Streaming, and write the processed data into Parquet files stored in an Amazon S3 bucket and finally use AWS Redshift as datawarehouse. It specifically focuses on two types of data: weather data and air quality data.
The data are stored in Redshift and ready to be used by data scientists or data analysis.


## Requirements
-Python 3.x
-AWS
-Docker (-Apache Spark 3.x,Kafka 0.10.x or later, Zookeeper)
