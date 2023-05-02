from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pymongo import MongoClient
from kafka import KafkaConsumer
import json

# Define the Kafka configuration properties
kafka_config = {
    "bootstrap_servers": "localhost:9092",
    "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
    "auto_offset_reset": "earliest",
    "enable_auto_commit": True,
}

# Define the MongoDB configuration properties
mongo_config = {
    "host": "localhost",
    "port": 27017,
    "username": "my_user",
    "password": "my_password",
    "authSource": "my_database",
    "authMechanism": "SCRAM-SHA-256",
}

# Define the PySpark configuration properties
spark_config = {
    "spark.app.name": "NoSQLPipeline",
    "spark.master": "local[*]",
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
}

# Define the MongoDB client object
mongo_client = MongoClient(**mongo_config)

# Define the PySpark dataframe schema
schema = "my_schema"

# Define the PySpark data transformation function
def transform_data(df):
    transformed_df = df.filter(col("my_column") > 10)
    return transformed_df

# Define the Kafka consumer object
consumer = KafkaConsumer("my_topic", **kafka_config)

# Define the PySpark dataframe reader object
spark = SparkSession.builder.appName("NoSQLPipeline").config(spark_config).getOrCreate()
df_reader = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "my_topic")

# Define the PySpark dataframe writer object
df_writer = df.writeStream.format("mongo").option("uri", "mongodb://my_user:my_password@localhost:27017/my_database.my_collection").option("database", "my_database").option("collection", "my_collection")

# Define the ETL pipeline
while True:
    for msg in consumer:
        # Extract data from Kafka
        data = msg.value

        # Transform data using PySpark
        df = df_reader.schema(schema).option("startingOffsets", "earliest").load().select(from_json(col("value"), schema).alias("data")).select("data.*")
        transformed_df = transform_data(df)

        # Load data into MongoDB
        df_writer.start().awaitTermination()
