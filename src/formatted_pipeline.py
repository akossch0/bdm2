import logging
import os
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession

from data_formatters import (
    AirQualityDataSource,
    AirQualityLookupDataSource,
    IdealistaDataSource,
    IncomeDataSource,
    LocationLookupDataSource,
)
from data_reconciliation import handle_non_reconciled_idealista_neighborhoods
from logging_config import configure_logger
from utils import log_env, setup_env, validate_env

logger = configure_logger(__name__, logging.INFO)

setup_env()
validate_env()
log_env()

MONGODB_HOST = os.environ.get("MONGODB_HOST")
MONGODB_PORT = os.environ.get("MONGODB_PORT")
MONGODB_URI = f"mongodb://{MONGODB_HOST}:{MONGODB_PORT}/"
MONGODB_DB = os.environ.get("MONGODB_DB")

MONGODB_LOCATION_LOOKUP_COLLECTION = os.environ.get("MONGODB_LOCATION_LOOKUP_COLLECTION")
MONGODB_LOCATION_LOOKUP_IDENTIFIERS = os.environ.get("MONGODB_LOCATION_LOOKUP_IDENTIFIERS").split(",")
MONGODB_AIRQUALITY_LOOKUP_COLLECTION = os.environ.get("MONGODB_AIRQUALITY_LOOKUP_COLLECTION")
MONGODB_AIRQUALITY_LOOKUP_IDENTIFIERS = os.environ.get("MONGODB_AIRQUALITY_LOOKUP_IDENTIFIERS").split(",")
MONGODB_INCOME_COLLECTION = os.environ.get("MONGODB_INCOME_COLLECTION")
MONGODB_INCOME_IDENTIFIERS = os.environ.get("MONGODB_INCOME_IDENTIFIERS").split(",")
MONGODB_IDEALISTA_COLLECTION = os.environ.get("MONGODB_IDEALISTA_COLLECTION")
MONGODB_IDEALISTA_IDENTIFIERS = os.environ.get("MONGODB_IDEALISTA_IDENTIFIERS").split(",")
MONGODB_AIRQUALITY_COLLECTION = os.environ.get("MONGODB_AIRQUALITY_COLLECTION")
MONGODB_AIRQUALITY_IDENTIFIERS = os.environ.get("MONGODB_AIRQUALITY_IDENTIFIERS").split(",")

SPARK_AVRO_JAR = os.environ.get("SPARK_AVRO_JAR")
SPARK_MONGO_CONNECTOR_JAR = os.environ.get("SPARK_MONGO_CONNECTOR_JAR")

LOOKUP_SOURCE_FOLDER = os.environ.get("LOOKUP_SOURCE_FOLDER")
INCOME_SOURCE_FOLDER = os.environ.get("INCOME_SOURCE_FOLDER")
IDEALISTA_SOURCE_FOLDER = os.environ.get("IDEALISTA_SOURCE_FOLDER")
AIRQUALITY_SOURCE_FOLDER = os.environ.get("AIRQUALITY_SOURCE_FOLDER")

CONF = (
    SparkConf()
    .set("spark.master", "local")
    .set("spark.app.name", "BDM Spark formatted pipeline")
    .set("spark.jars.packages", f"{SPARK_AVRO_JAR},{SPARK_MONGO_CONNECTOR_JAR}")
    .set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
    .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties")
    .set("spark.mongodb.read.connection.uri", MONGODB_URI)
    .set("spark.mongodb.write.connection.uri", MONGODB_URI)
)

if __name__ == "__main__":
    spark = SparkSession.builder.config(conf=CONF).getOrCreate()
    logger.info(f"Python version = {spark.sparkContext.pythonVer}")
    logger.info(f"Spark version = {spark.version}")

    location_lookup = LocationLookupDataSource(
        source_folder=LOOKUP_SOURCE_FOLDER,
        spark=spark,
        target_db=MONGODB_DB,
        target_collection=MONGODB_LOCATION_LOOKUP_COLLECTION,
        identifiers=MONGODB_LOCATION_LOOKUP_IDENTIFIERS,
    )

    airquality_lookup = AirQualityLookupDataSource(
        source_folder=LOOKUP_SOURCE_FOLDER,
        spark=spark,
        target_db=MONGODB_DB,
        target_collection=MONGODB_AIRQUALITY_LOOKUP_COLLECTION,
        identifiers=MONGODB_AIRQUALITY_LOOKUP_IDENTIFIERS,
    )

    income = IncomeDataSource(
        source_folder=INCOME_SOURCE_FOLDER,
        spark=spark,
        target_db=MONGODB_DB,
        target_collection=MONGODB_INCOME_COLLECTION,
        identifiers=MONGODB_INCOME_IDENTIFIERS,
    )

    idealista = IdealistaDataSource(
        source_folder=IDEALISTA_SOURCE_FOLDER,
        spark=spark,
        target_db=MONGODB_DB,
        target_collection=MONGODB_IDEALISTA_COLLECTION,
        identifiers=MONGODB_IDEALISTA_IDENTIFIERS,
    )

    airquality = AirQualityDataSource(
        source_folder=AIRQUALITY_SOURCE_FOLDER,
        spark=spark,
        target_db=MONGODB_DB,
        target_collection=MONGODB_AIRQUALITY_COLLECTION,
        identifiers=MONGODB_AIRQUALITY_IDENTIFIERS,
    )

    sources = [location_lookup, airquality_lookup, income, idealista, airquality]
    pipeline_start_time = time.time()
    logger.info(f"Starting the pipeline at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(pipeline_start_time))}")
    for source in sources:
        source_name = source.__class__.__name__
        logger.info(f"Starting to process {source_name}")
        process_start_time = time.time()

        source.format_and_load_data()

        process_end_time = time.time()
        minutes, seconds = divmod(process_end_time - process_start_time, 60)
        logger.info(f"Finished processing {source_name} in {int(minutes)} minutes and {int(seconds)} seconds")

    logger.info("Starting the reconciliation process")
    reconciliation_start_time = time.time()
    handle_non_reconciled_idealista_neighborhoods(spark)
    reconciliation_end_time = time.time()
    minutes, seconds = divmod(reconciliation_end_time - reconciliation_start_time, 60)
    logger.info(f"Finished the reconciliation process in {int(minutes)} minutes and {int(seconds)} seconds")

    minutes, seconds = divmod(time.time() - pipeline_start_time, 60)
    logger.info(f"Finished the formatted pipeline in {int(minutes)} minutes and {int(seconds)} seconds")
    spark.stop()
