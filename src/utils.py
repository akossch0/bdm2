import logging
import os
import sys

import ftfy
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from logging_config import configure_logger

logger = configure_logger(__name__, logging.ERROR)


def setup_env():
    load_dotenv()
    SPARK_HOME = os.getenv("SPARK_HOME")
    JAVA_HOME = os.getenv("JAVA_HOME")
    SRC_PATH = os.getenv("SRC_PATH")
    # Set the environment variables
    os.environ["PATH"] = os.pathsep.join(
        [
            os.path.join(SPARK_HOME, "bin"),
            os.environ["PATH"],
        ]
    )
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    if "PYTHONPATH" not in os.environ:
        os.environ["PYTHONPATH"] = ""
    if SRC_PATH not in os.environ["PYTHONPATH"]:
        if "PYTHONPATH" in os.environ:
            os.environ["PYTHONPATH"] = os.pathsep.join(
                [
                    os.environ["PYTHONPATH"],
                    os.path.join(os.getcwd(), "src"),
                ]
            )


def validate_env():
    SPARK_HOME = os.getenv("SPARK_HOME")
    JAVA_HOME = os.getenv("JAVA_HOME")
    if not os.path.exists(SPARK_HOME):
        raise ValueError(f"SPARK_HOME path does not exist: {SPARK_HOME}")
    if not os.path.exists(JAVA_HOME):
        raise ValueError(f"JAVA_HOME path does not exist: {JAVA_HOME}")

    if not any(SPARK_HOME in path for path in os.environ["PATH"].split(os.pathsep)):
        raise ValueError(f"PATH does not contain {SPARK_HOME}/bin")

    if not os.environ["PYSPARK_PYTHON"]:
        raise ValueError("PYSPARK_PYTHON is not set")
    if not os.environ["PYSPARK_DRIVER_PYTHON"]:
        raise ValueError("PYSPARK_DRIVER_PYTHON is not set")


def log_env():
    """
    Log the environment variables. Debug level logging is used to avoid printing the environment variables in the console.
    """
    logger.info(f"SPARK_HOME = {os.environ['SPARK_HOME']}")
    logger.info(f"JAVA_HOME = {os.environ['JAVA_HOME']}")
    # logger.debug(f"PATH = {os.environ['PATH']}")
    logger.info(f"PYSPARK_PYTHON = {os.environ['PYSPARK_PYTHON']}")
    logger.info(f"PYSPARK_DRIVER_PYTHON = {os.environ['PYSPARK_DRIVER_PYTHON']}")


def fix_encoding(s: pd.Series) -> pd.Series:
    def fix_value(x):
        if x is not None:
            try:
                # Attempt to fix common double-encoding issues
                fixed = x.encode("latin1").decode("utf-8")
                return fixed
            except Exception as e:
                logger.warn(f"Failed decoding: {e}. Trying ftfy...")

            try:
                # Attempt to fix using ftfy
                fixed = ftfy.fix_text(x)
                return fixed
            except Exception as e:
                logger.error(f"ftfy failed: {e}. Returning original string.")
                return x
        return x

    return s.apply(fix_value)


def flatten_dataframe(df: DataFrame) -> DataFrame:
    # Check for nested fields in DataFrame schema
    complex_fields = [
        f.name
        for f in df.schema.fields
        if f.dataType.simpleString().startswith("struct")
    ]

    while complex_fields:
        col_exprs = []
        for field in df.schema.fields:
            if field.dataType.simpleString().startswith("struct"):
                # Add nested fields to the column expression list
                nested_fields = field.dataType.fields
                col_exprs += [
                    col(f"{field.name}.{nested_field.name}").alias(
                        f"{field.name}_{nested_field.name}"
                    )
                    for nested_field in nested_fields
                ]
            else:
                # Retain non-nested fields as is
                col_exprs.append(col(field.name))

        # Select all columns based on the constructed column expressions
        df = df.select(*col_exprs)
        # Check again if there are nested fields after transformation
        complex_fields = [
            f.name
            for f in df.schema.fields
            if f.dataType.simpleString().startswith("struct")
        ]

    return df


def str_to_float(value: str) -> float:
    converted = None
    try:
        converted = float(value)
    except:
        converted = None
    return converted


def jaccard_similarity(str1, str2):
    if str1 is None or str2 is None:
        return 0.0
    str1 = str1.lower()
    str2 = str2.lower()
    set1 = set(str1.split())
    set2 = set(str2.split())
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    if union == 0:
        return 0.0
    return float(intersection) / union


def write_with_jdbc(
    df: DataFrame, table_name: str, jdbc_url: str, connection_props: dict
) -> None:
    df.write.jdbc(
        url=jdbc_url, table=table_name, mode="append", properties=connection_props
    )
