from pyspark import SparkConf
from pyspark.sql import SparkSession

from utils import log_env, setup_env, validate_env

setup_env()
validate_env()
log_env()

# Create the configuration in the local machine and give a name to the application
conf = SparkConf().set("spark.master", "local").set("spark.app.name", "Spark Dataframes Tutorial")

# Create the session
spark = SparkSession.builder.config(conf=conf).config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.1").getOrCreate()
print(f"Python version = {spark.sparkContext.pythonVer}")
print(f"Spark version = {spark.version}")
print(spark.sparkContext.getConf().getAll())

df = spark.read.format("avro").load(
    "data/persistent-landing-zone/lookup/2018_qualitat_aire_estacions_49d67292d3b070494d872ad69f21f33606716bb9.avro"
)

# print the schema
df.printSchema()

df.show()

spark.stop()
