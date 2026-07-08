from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro, to_avro
from pyspark.sql.types import *
from dotenv import load_dotenv
from confluent_kafka.schema_registry import SchemaRegistryClient
# import findspark
import os

# findspark.init()

# .config("spark.jars.package", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.apache.spark:spark-avro_2.12:3.5.0"]
spark = SparkSession.builder\
            .appName("Ad stream")\
            .config("spark.sql.shuffle.partitions", "2")\
            .config("spark.streaming.stopGracefullyOnShutdown", "true")\
            .config("spark.jars.package", ",".join(packages))\
            .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")
load_dotenv()
# Define Kafka configuration
kafka_id = os.environ.get("confluent_kafka_id")
kafka_secret_key = os.environ.get("confluent_kafka_secret_key")
topic = "ad_topic"

url = 'https://psrc-kjwmg.ap-southeast-2.aws.confluent.cloud'
schema_id = os.environ.get("confluence_schema_id")
schema_secret = os.environ.get("confluence_schema_secret")
schema_registry_client = SchemaRegistryClient({
  'url': url,
  'basic.auth.user.info': '{}:{}'.format(schema_id, schema_secret)
})
subject_name = f"{topic}-value"
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

kafka_options = {
    'kafka.bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.security.protocol': 'SASL_SSL',
    "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_id}' password='{kafka_secret_key}';",
    "kafka.ssl.endpoint.identification.algorithm": "https",
    "subscribe": topic,
    "startingOffsets": "earliest"
}

# schema = StructType([
#     StructField("ad_id", StringType()),
#     StructField("timestamp", TimestampType()),
#     StructField("clicks", IntegerType()),
#     StructField("views", IntegerType()),
#     StructField("costs", FloatType())
# ])

with open("mock_data_schema.json", "r") as f:
    json_format_schema = f.read()

df = spark.readStream.format("kafka")\
    .options(**kafka_options)\
    .load()
df.printSchema()

data = df\
        .select(from_avro("value", schema_str).alias("ctr"))\
        .select("ctr.*")
# df = df.groupBy("ad_id", window(df.timestamp, "1 minute"))\
#     .agg(count("clicks").alias("total_clicks"))
print(schema_str)
query = data.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="3 second") \
    .start()\
    .awaitTermination()