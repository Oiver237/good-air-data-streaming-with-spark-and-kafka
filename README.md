# good-air-data-streaming-with-spark-and-kafka

![Archi-data-streaming](https://github.com/Oiver237/good-air-data-streaming-with-spark-and-kafka/assets/73575249/d7a753d5-8f2b-45c7-a84c-42646fa41c93)

### FR
Ce pipeline Spark vise à ingérer en streaming les données météorologiques et les données de qualité de l'air des 10 plus grandes villes de France à partir de sujets Apache Kafka, à les traiter à l'aide de Spark Structured Streaming, à écrire les données traitées dans des fichiers Parquet stockés dans un bucket Amazon S3, et enfin à utiliser AWS Redshift comme datawarehouse. Les données sont stockées dans Redshift et prêtes à être utilisées par les data scientites ou les data analystes.

### EN
This Spark Streaming pipeline aims to ingest weather data and air quality data of the 10 biggest cities in France from Apache Kafka topics, process it using Spark Structured Streaming, and write the processed data into Parquet files stored in an Amazon S3 bucket and finally use AWS Redshift as datawarehouse.
The data are stored in Redshift and ready to be used by data scientists or data analysis.


## Requirements
- Python 3.x
- AWS
- Docker (Apache Spark 3.x,Kafka 0.10.x or later, Zookeeper)
