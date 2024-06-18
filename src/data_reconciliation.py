import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import DoubleType

from logging_config import configure_logger
from src.utils import jaccard_similarity

logger = configure_logger(__name__, logging.INFO)

MONGODB_DB = os.environ.get("MONGODB_DB")
MONGODB_IDEALISTA_COLLECTION = os.environ.get("MONGODB_IDEALISTA_COLLECTION")
MONGODB_LOCATION_LOOKUP_COLLECTION = os.environ.get("MONGODB_LOCATION_LOOKUP_COLLECTION")


def handle_non_reconciled_idealista_neighborhoods(spark: SparkSession):
    idealista_df = spark.read.format("mongodb").option("database", MONGODB_DB).option("collection", MONGODB_IDEALISTA_COLLECTION).load()

    location_lookup_df = (
        spark.read.format("mongodb").option("database", MONGODB_DB).option("collection", MONGODB_LOCATION_LOOKUP_COLLECTION).load()
    )

    logger.info(f"Joining idealista with location_lookup")
    location_lookup_df = location_lookup_df.withColumnsRenamed({"neighborhood": "neighborhood_", "district": "district_"})
    idealista_df_looked_up = idealista_df.filter("municipality = 'Barcelona'").join(
        location_lookup_df,
        idealista_df.neighborhood == location_lookup_df.neighborhood_n_reconciled,
        "left",
    )

    logger.info(f"There are {idealista_df_looked_up.count()} listings in Barcelona")
    logger.info(f"There are {idealista_df_looked_up.filter('neighborhood_id is null').count()} listings with non-reconciled neighborhoods")

    problematic_neighborhoods = idealista_df_looked_up.filter("neighborhood_id is null").select("neighborhood").distinct()
    logger.info(f"There are {problematic_neighborhoods.count()} unique non-reconciled neighborhoods")

    jaccard_similarity_udf = udf(jaccard_similarity, DoubleType())

    logger.info(f"Calculating jaccard similarity between non-reconciled neighborhoods and location_lookup neighborhoods to find matches")
    cross_joined_df = problematic_neighborhoods.crossJoin(location_lookup_df)
    cross_joined_df = cross_joined_df.withColumn(
        "jaccard_similarity",
        jaccard_similarity_udf(col("neighborhood"), col("neighborhood_n_reconciled")),
    )
    similarity_threshold = 0.3
    filtered_df = cross_joined_df.filter(col("jaccard_similarity") >= similarity_threshold)
    non_reconciled_locations = filtered_df.select(
        "neighborhood",
        "neighborhood_n_reconciled",
        "neighborhood_id",
        "jaccard_similarity",
    )

    logger.info(
        f"There are {non_reconciled_locations.distinct().count()} unique non-reconciled neighborhoods after filtering with jaccard similarity >= {similarity_threshold}"
    )

    logger.info(f"Sorting non-reconciled neighborhoods by jaccard similarity and dropping duplicates")
    non_reconciled_locations = non_reconciled_locations.orderBy("jaccard_similarity", ascending=False).dropDuplicates(["neighborhood"])

    # manual correction of row with neighborhood 'El Gòtic' to id of Q941385
    logger.info(f"Manually correcting neighborhood 'El Gòtic' to id Q941385")
    non_reconciled_locations = non_reconciled_locations.withColumn(
        "neighborhood_id",
        when(col("neighborhood") == "El Gòtic", "Q17154").otherwise(col("neighborhood_id")),
    )

    # add two entries: 'El Poble Sec - Parc de Montjuïc' neitghborhood with id Q980253 and 'Horta' neighborhood with id Q15225338
    logger.info(f"Adding 'El Poble Sec - Parc de Montjuïc' neighborhood with id Q980253 and 'Horta' neighborhood with id Q15225338")
    manual_records = spark.createDataFrame(
        [
            ("El Poble Sec - Parc de Montjuïc", "el Poble Sec", "Q980253", 1.0),
            ("Horta", "Horta", "Q15225338", 1.0),
        ],
        [
            "neighborhood",
            "neighborhood_id",
            "neighborhood_n_reconciled",
            "jaccard_similarity",
        ],
    )
    non_reconciled_locations = non_reconciled_locations.union(manual_records)

    non_reconciled_locations = non_reconciled_locations.withColumnsRenamed(
        {"neighborhood_id": "neighborhood_id_", "neighborhood": "neighborhood_"}
    )

    resolved_neighborhoods = non_reconciled_locations.select("neighborhood_")
    diff = problematic_neighborhoods.join(
        resolved_neighborhoods,
        problematic_neighborhoods.neighborhood == resolved_neighborhoods.neighborhood_,
        "leftanti",
    )
    logger.info(f"There are {diff.count()} neighborhoods that could not be resolved")
    if diff.count() > 0:
        diff.show()

    logger.info(f"Saving non-reconciled neighborhoods to mongodb")
    non_reconciled_locations.write.format("mongodb").option("database", MONGODB_DB).option(
        "collection", "idealista_resolved_location_lookup"
    ).mode("overwrite").save()
