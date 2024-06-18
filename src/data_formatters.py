import logging
import os
import time
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, col, lit, pandas_udf, udf
from pyspark.sql.types import DoubleType, IntegerType, NullType, NumericType, StringType, StructType

from logging_config import configure_logger
from utils import fix_encoding, flatten_dataframe, str_to_float

logger = configure_logger(__name__, logging.INFO)


class BaseDataSource:
    def __init__(
        self,
        source_folder: str,
        spark: SparkSession,
        target_db: str,
        target_collection: str,
        identifiers: List[str],
        delete_outliers: bool = False,
    ) -> None:
        self.source_folder = source_folder
        self.spark = spark
        self.target_db = target_db
        self.target_collection = target_collection
        self.identifiers = identifiers
        self.delete_outliers = delete_outliers
        self.dfs = (
            {}
        )  # This will store the dataframes that we will load in the set_up method. This way we can access them later in the pipeline.
        self.merged_df = None
        self.outliers = {}

        self.set_up()

    def set_up(self) -> None:
        # should not be called on this class
        raise NotImplementedError

    def format_and_load_data(self) -> None:
        raise NotImplementedError

    def lowercase_columns(self) -> None:
        for key in self.dfs:
            df = self.dfs[key]
            for col in df.columns:
                df = df.withColumnRenamed(col, col.lower())
            self.dfs[key] = df

    def flatten_dataframes(self) -> None:
        self.dfs = {key: flatten_dataframe(df) for key, df in self.dfs.items()}

    def perform_custom_formatting(
        self,
        temporal_column_name: str,
        manual_column_types: dict = None,
        cast_all: bool = False,
    ) -> None:
        """
        Perform custom formatting on a collection of dataframes. This includes adding missing columns,
        checking data type consistency, and adding a temporal column.

        :param self: self
        :param temporal_column_name: The name of the temporal column to add to each dataframe.
        :param manual_column_types: A dictionary of column names and their data types for manual casting.
        :param cast_all: A boolean flag to indicate whether to cast all columns.
        :return: None
        """

        class_name = self.__class__.__name__

        def get_nested_fields():
            """
            Get nested fields from the dataframes.

            :return: A set of nested field names.
            """
            nested = set()
            for df in self.dfs.values():
                nested.update({field.name for field in df.schema.fields if isinstance(field.dataType, StructType)})
            return nested

        def get_all_columns(nested_fields):
            """
            Get all columns from the dataframes excluding nested fields.

            :param nested_fields: A set of nested field names.
            :return: A set of all column names.
            """
            all_cols = set()
            for df in self.dfs.values():
                all_cols.update(df.columns)
            return all_cols - nested_fields

        def get_column_data_types(manual_types: dict = None):
            """
            Get the data types for each column in the dataframes.

            :param manual_types: A dictionary of column names and their data types for manual casting.
            :return: A dictionary of column names and their data types.
            """
            col_types = {}
            for df in self.dfs.values():
                for col in df.columns:
                    if col not in col_types and df.schema[col].dataType != NullType():
                        col_types[col] = df.schema[col].dataType
            if manual_types:
                for col, dtype in manual_types.items():
                    col_types[col] = dtype
            return col_types

        def log_missing_columns_info():
            """
            Log information about missing columns in the dataframes.

            :return: A dictionary of days and their corresponding missing columns.
            """
            missing_cols_info = {}
            for day, df in self.dfs.items():
                missing_cols = all_columns - set(df.columns)
                if missing_cols:
                    missing_cols_info[day] = missing_cols
                    logger.debug(
                        f"{class_name}: There are {len(missing_cols)} missing columns in the dataframe for day {day}, the columns are: {missing_cols}"
                    )
            return missing_cols_info

        def add_missing_columns():
            """
            Add missing columns to the dataframes with null values.

            :return: None
            """
            for day in self.dfs:
                df = self.dfs[day]
                for col in all_columns - set(df.columns):
                    df = df.withColumn(col, lit(None))
                self.dfs[day] = df.select(sorted(all_columns))

        def check_data_type_consistency():
            """
            Check the consistency of data types across the dataframes.

            :return: A dictionary of column names and their inconsistent data types.
            """
            dtype_counter = {}
            for df in self.dfs.values():
                for col in df.columns:
                    dtype = df.schema[col].dataType
                    dtype_counter[f"{col} - {dtype}"] = dtype_counter.get(f"{col} - {dtype}", 0) + 1
            return {col_type: count for col_type, count in dtype_counter.items() if count != len(self.dfs)}

        def cast_columns_to_correct_type(manual_column_to_cast: list = None, cast_all: bool = False):
            """
            Cast columns to their correct data types.

            :param manual_column_to_cast: A list of columns to cast manually.
            :param cast_all: A boolean flag to indicate whether to cast all columns.
            :return: None
            """
            for day in self.dfs:
                df = self.dfs[day]
                for col in df.columns:
                    if cast_all or df.schema[col].dataType == NullType() or col in manual_column_to_cast:
                        df = df.withColumn(col, df[col].cast(column_data_types[col]))
                self.dfs[day] = df

        def add_temporal_column(temporal_column: str):
            """
            Add a temporal column to each dataframe.

            :param temporal_column: The name of the temporal column to add.
            :return: None
            """
            for key in self.dfs:
                self.dfs[key] = self.dfs[key].withColumn(temporal_column, lit(key))

        nested_fields = get_nested_fields()
        all_columns = get_all_columns(nested_fields)
        logger.debug(f"{class_name}: There are {len(all_columns)} unique columns in the dataframes")

        column_data_types = get_column_data_types(manual_column_types)

        missing_columns_info = log_missing_columns_info()
        nr_of_incomplete_dfs = len(missing_columns_info)
        frequently_missing_cols = set.union(*missing_columns_info.values()) if missing_columns_info else set()
        logger.debug(f"{class_name}: There are {nr_of_incomplete_dfs} incomplete dataframes out of {len(self.dfs)}")
        logger.debug(f"{class_name}: There are {len(frequently_missing_cols)} frequently missing columns: {frequently_missing_cols}")
        if nr_of_incomplete_dfs > 0:
            logger.debug(f"{class_name}: Creating missing columns with null values...")
            add_missing_columns()

            missing_columns_info = log_missing_columns_info()
            nr_of_incomplete_dfs = len(missing_columns_info)
            frequently_missing_cols = set.union(*missing_columns_info.values()) if missing_columns_info else set()
            logger.debug(
                f"{class_name}: There are {nr_of_incomplete_dfs} incomplete dataframes out of {len(self.dfs)} after adding missing columns"
            )
            logger.debug(f"{class_name}: There are {len(frequently_missing_cols)} frequently missing columns: {frequently_missing_cols}")
            if nr_of_incomplete_dfs > 0:
                raise ValueError(
                    f"{class_name}: There are still incomplete dataframes after adding missing columns: {missing_columns_info}"
                )

        logger.debug(f"{class_name}: Checking data type consistency...")
        columns_needing_fix = check_data_type_consistency()
        nr_cols_needing_fix = len({col.split(" - ")[0] for col in columns_needing_fix})
        logger.debug(f"{class_name}: There are {nr_cols_needing_fix} columns that need fixing: {columns_needing_fix}")
        if nr_cols_needing_fix > 0:
            logger.debug(f"{class_name}: Casting to correct data type...")
            cast_columns_to_correct_type(
                list(manual_column_types.keys()) if manual_column_types else None,
                cast_all,
            )

            columns_needing_fix = check_data_type_consistency()
            nr_cols_needing_fix = len({col.split(" - ")[0] for col in columns_needing_fix})
            logger.debug(
                f"{class_name}: There are {nr_cols_needing_fix} columns that need fixing: {columns_needing_fix} after casting to correct data type"
            )
            if nr_cols_needing_fix > 0:
                raise ValueError(
                    f"{class_name}: There are still columns that need fixing after casting to correct data type: {columns_needing_fix}"
                )

        add_temporal_column(temporal_column_name)

    def union_dataframes(self) -> None:
        if type(self.dfs) == list:
            self.merged_df = self.dfs[0]
            for df in self.dfs[1:]:
                self.merged_df = self.merged_df.union(df)
        elif type(self.dfs) == dict:
            # merge all dataframes
            for key in self.dfs:
                if self.merged_df is None:
                    self.merged_df = self.dfs[key]
                else:
                    self.merged_df = self.merged_df.union(self.dfs[key])
        else:
            raise ValueError("Dataframes must be a list or a dictionary.")

    def detect_continuous_variables(self, distinct_threshold: int, drop_vars: List[str] = []) -> list[str]:
        continuous_columns = []
        for column in self.merged_df.drop(*drop_vars).columns:
            dtype = self.merged_df.schema[column].dataType
            if isinstance(dtype, (IntegerType, NumericType, DoubleType)):
                distinct_count = self.merged_df.select(approx_count_distinct(column)).collect()[0][0]
                if distinct_count > distinct_threshold:
                    continuous_columns.append(column)
        return continuous_columns

    def iqr_outlier_treatment(self, columns, factor=1.5, delete_outliers=False) -> None:
        """
        Detects and treats outliers using IQR for multiple variables in a PySpark DataFrame.

        :param self: self
        :param columns: A list of columns to apply IQR outlier treatment
        :param factor: The IQR factor to use for detecting outliers (default is 1.5)
        :return: The processed DataFrame with outliers treated
        """
        class_name = self.__class__.__name__
        all_outliers = {}
        for column in columns:
            quantiles = self.merged_df.approxQuantile(column, [0.25, 0.75], 0.01)
            q1, q3 = quantiles[0], quantiles[1]
            iqr = q3 - q1

            lower_bound = q1 - factor * iqr
            upper_bound = q3 + factor * iqr

            outliers = self.merged_df.filter((col(column) < lit(lower_bound)) | (col(column) > lit(upper_bound)))
            all_outliers[column] = outliers
            # num_outliers = outliers.count()
            logger.debug(
                f"{class_name}: Computing outliers for column {column} with lower bound {lower_bound} and upper bound {upper_bound}..."
            )

            # Filter outliers and update the DataFrame
            if delete_outliers:
                logger.debug(f"{class_name}: Deleting outliers in column {column}.")
                self.merged_df = self.merged_df.filter((col(column) >= lit(lower_bound)) & (col(column) <= lit(upper_bound)))
        self.outliers = all_outliers

    def log_outliers(self):
        if not self.outliers:
            return
        if not os.path.exists("logs"):
            os.makedirs("logs")
        if not os.path.exists("logs/outliers"):
            os.makedirs("logs/outliers")
        now = time.time()
        file_name = f"logs/outliers/{self.target_collection}_{now}.txt"
        with open(file_name, "w") as f:
            for column in self.outliers:
                f.write(f"Column: {column}\n")
                f.write(f"Number of outliers: {self.outliers[column].count()}\n")
                csv_filename = file_name.replace(".txt", f"_{column}.csv")
                outliers = self.outliers[column]
                outliers.write.format("csv").save(csv_filename)

    def de_duplicate(self, columns: List[str] = None) -> None:
        logger.info("Size before de-duplication: " + str(self.merged_df.count()))
        if not columns:
            self.merged_df = self.merged_df.dropDuplicates()
        else:
            self.merged_df = self.merged_df.dropDuplicates(subset=columns)
        logger.info("Size after de-duplication: " + str(self.merged_df.count()))

    def fix_str_encoding(self):
        if not self.merged_df:
            raise ValueError("No merged dataframe to fix encoding for.")

        fix_encoding_udf = pandas_udf(fix_encoding, StringType())
        for column in self.merged_df.columns:
            if self.merged_df.schema[column].dataType == StringType():
                self.merged_df = self.merged_df.withColumn(column, fix_encoding_udf(column))

    def upsert_formatted_db(self, collection_name: str = None) -> None:
        write_config = {
            "writeConcern.w": "majority",
            "replaceDocument": "false",
            "idFieldList": ",".join(self.identifiers),  # Specify the field to use for upsert
            "operationType": "update",  # Specify the update operation
            "upsertDocument": "true",  # Enable upsert logic
        }
        collection_n = collection_name if collection_name else self.target_collection
        self.merged_df.write.format("mongodb").mode("append").option("database", self.target_db).option("collection", collection_n).options(
            **write_config
        ).save()


class LocationLookupDataSource(BaseDataSource):

    def set_up(self) -> None:
        files = os.listdir(self.source_folder)
        files = [file for file in files if file.startswith("idealista") or file.startswith("income")]
        files = [f"{self.source_folder}/{file}" for file in files]
        self.dfs = []
        for file in files:
            df = self.spark.read.format("avro").load(file)
            self.dfs.append(df)

    def format_and_load_data(self) -> None:
        class_name = self.__class__.__name__
        # schema alignment not needed with simple lookup tables
        # union
        logger.info(f"{class_name}: Union dataframes...")
        self.union_dataframes()
        logger.info(f"{class_name}: Union complete.")

        # de-duplicate
        logger.info(f"{class_name}: De-duplicating data...")
        self.de_duplicate(self.identifiers)
        logger.info(f"{class_name}: De-duplication complete.")

        # fix encoding
        logger.info(f"{class_name}: Fixing string encoding...")
        self.fix_str_encoding()
        logger.info(f"{class_name}: String encoding fixed.")

        # upsert
        logger.info(f"{class_name}: Upserting data to MongoDB...")
        self.upsert_formatted_db()
        logger.info(f"{class_name}: Upsert complete.")


class AirQualityLookupDataSource(BaseDataSource):

    def set_up(self):
        files = os.listdir(self.source_folder)
        files = [file for file in files if file[:4].isdigit()]
        years = [file[:4] for file in files]
        files = [f"{self.source_folder}/{file}" for file in files]
        self.dfs = {}
        for file, year in zip(files, years):
            df = self.spark.read.format("avro").load(file)
            self.dfs[year] = df

    def perform_custom_formatting(self) -> None:
        # add a year column to each dataframe
        for key in self.dfs:
            df = self.dfs[key]
            df = df.withColumn("year", lit(int(key)))
            self.dfs[key] = df

    def format_and_load_data(self) -> None:
        class_name = self.__class__.__name__
        # schema alignment
        logger.info(f"{class_name}: Lowercasing columns...")
        self.lowercase_columns()
        logger.info(f"{class_name}: Columns lowercased.")

        # custom formatting
        logger.info(f"{class_name}: Performing custom formatting...")
        self.perform_custom_formatting()
        logger.info(f"{class_name}: Custom formatting complete.")

        # separate handling of 2018 data
        logger.info(f"{class_name}: Handling 2018 data separately...")
        self.merged_df = self.dfs.pop("2018")
        self.de_duplicate(self.identifiers)
        self.fix_str_encoding()
        self.upsert_formatted_db(self.target_collection + "_2018")
        logger.info(f"{class_name}: 2018 data handled.")
        self.merged_df = None

        # union
        logger.info(f"{class_name}: Union dataframes...")
        self.union_dataframes()
        logger.info(f"{class_name}: Union complete.")

        # de-duplicate
        logger.info(f"{class_name}: De-duplicating data...")
        self.de_duplicate(self.identifiers)
        logger.info(f"{class_name}: De-duplication complete.")

        # fix encoding
        logger.info(f"{class_name}: Fixing string encoding...")
        self.fix_str_encoding()
        logger.info(f"{class_name}: String encoding fixed.")

        # upsert
        logger.info(f"{class_name}: Upserting data to MongoDB...")
        self.upsert_formatted_db()
        logger.info(f"{class_name}: Upsert complete.")


class IncomeDataSource(BaseDataSource):

    def set_up(self):
        files = os.listdir(self.source_folder)
        files = [file for file in files if file[:4].isdigit()]
        years = [file[:4] for file in files]
        files = [f"{self.source_folder}/{file}" for file in files]
        self.dfs = {}
        for file, year in zip(files, years):
            df = self.spark.read.format("avro").load(file)
            self.dfs[year] = df

    def perform_custom_formatting(self) -> None:
        # filter out from all dfs where codi_districte is not in the range of 1 and 10, and codi_barri is not in the range of 1 and 73
        for year in self.dfs:
            df = self.dfs[year]
            df = df.filter(df["codi_districte"].between(1, 10) & df["codi_barri"].between(1, 73))
            self.dfs[year] = df

        str_to_float_udf = udf(str_to_float, DoubleType())
        # apply the udf to the column named 'índex rfd barcelona = 100'
        for year in self.dfs:
            self.dfs[year] = self.dfs[year].withColumn(
                "índex rfd barcelona = 100",
                str_to_float_udf(col("índex rfd barcelona = 100")),
            )

    def format_and_load_data(self) -> None:
        class_name = self.__class__.__name__
        # schema alignment
        logger.info(f"{class_name}: Lowercasing columns...")
        self.lowercase_columns()
        logger.info(f"{class_name}: Columns lowercased.")

        # custom formatting
        logger.info(f"{class_name}: Performing custom formatting...")
        self.perform_custom_formatting()
        logger.info(f"{class_name}: Custom formatting complete.")

        # union
        logger.info(f"{class_name}: Union dataframes...")
        self.union_dataframes()
        logger.info(f"{class_name}: Union complete.")

        # de-duplicate
        logger.info(f"{class_name}: De-duplicating data...")
        self.de_duplicate(self.identifiers)
        logger.info(f"{class_name}: De-duplication complete.")

        # fix encoding
        logger.info(f"{class_name}: Fixing string encoding...")
        self.fix_str_encoding()
        logger.info(f"{class_name}: String encoding fixed.")

        # outlier treatment
        if self.delete_outliers:
            logger.info(f"{class_name}: Deleting univariate outliers...")
        else:
            logger.info(f"{class_name}: Logging univariate outliers...")
        columns = self.detect_continuous_variables(10, drop_vars=["codi_districte", "codi_barri"])
        self.iqr_outlier_treatment(columns, factor=3.0, delete_outliers=self.delete_outliers)
        self.log_outliers()
        logger.info(f"{class_name}: Done.")

        # upsert
        logger.info(f"{class_name}: Upserting data to MongoDB...")
        self.upsert_formatted_db()
        logger.info(f"{class_name}: Upsert complete.")


class IdealistaDataSource(BaseDataSource):

    def set_up(self):
        files = os.listdir(self.source_folder)
        files = [file for file in files if file[:4].isdigit()]
        days = [file[:10] for file in files]
        files = [f"{self.source_folder}/{file}" for file in files]
        self.dfs = {}
        for file, day in zip(files, days):
            df = self.spark.read.format("avro").load(file)
            self.dfs[day] = df

    def format_and_load_data(self) -> None:
        class_name = self.__class__.__name__
        # schema alignment
        logger.info(f"{class_name}: Lowercasing columns, flatterning dataframe...")
        self.lowercase_columns()
        self.flatten_dataframes()
        logger.info(f"{class_name}: Done.")

        # custom formatting
        logger.info(f"{class_name}: Performing custom formatting...")
        # call super class method
        self.perform_custom_formatting("day", {"distance": IntegerType()})
        logger.info(f"{class_name}: Custom formatting complete.")

        # union
        logger.info(f"{class_name}: Union dataframes...")
        self.union_dataframes()
        logger.info(f"{class_name}: Union complete.")

        # de-duplicate
        logger.info(f"{class_name}: De-duplicating data...")
        self.de_duplicate(self.identifiers)
        logger.info(f"{class_name}: De-duplication complete.")

        # fix encoding
        logger.info(f"{class_name}: Fixing string encoding...")
        self.fix_str_encoding()
        logger.info(f"{class_name}: String encoding fixed.")

        # outlier treatment
        if self.delete_outliers:
            logger.info(f"{class_name}: Deleting univariate outliers...")
        else:
            logger.info(f"{class_name}: Logging univariate outliers...")
        columns = self.detect_continuous_variables(10, drop_vars=["codi_districte", "codi_barri"])
        self.iqr_outlier_treatment(columns, factor=3.0, delete_outliers=self.delete_outliers)
        self.log_outliers()
        logger.info(f"{class_name}: Done.")

        # upsert
        logger.info(f"{class_name}: Upserting data to MongoDB...")
        self.upsert_formatted_db()
        logger.info(f"{class_name}: Upsert complete.")


class AirQualityDataSource(BaseDataSource):

    def set_up(self):
        files = os.listdir(self.source_folder)
        files = [file for file in files if file[:4].isdigit()]
        months = [file[:7] for file in files]
        files = [f"{self.source_folder}/{file}" for file in files]
        self.dfs = {}
        for file, month in zip(files, months):
            df = self.spark.read.format("avro").load(file)
            self.dfs[month] = df

    def format_and_load_data(self) -> None:
        class_name = self.__class__.__name__
        # schema alignment
        logger.info(f"{class_name}: Lowercasing columns...")
        self.lowercase_columns()
        logger.info(f"{class_name}: Columns lowercased.")

        logger.info(f"{class_name}: Splitting dataframes into old and new dataframes...")
        # filter dfs with less than or equal to 1 columns
        self.dfs = {month: df for month, df in self.dfs.items() if len(df.columns) > 1}
        dfs_old = {month: self.dfs[month] for month in self.dfs if month <= "2019_03"}
        dfs_new = {month: self.dfs[month] for month in self.dfs if month > "2019_03"}
        logger.info(f"{class_name}: Splitting complete. There are {len(dfs_old)} old dataframes and {len(dfs_new)} new dataframes.")
        old_identifiers = ["codi_eoi", "codi_dtes", "datetime"]
        new_identifiers = self.identifiers
        for i, dfs in enumerate([dfs_old, dfs_new]):
            data_type = None
            if i == 0:
                data_type = "old"
                self.identifiers = old_identifiers
                continue
            else:
                data_type = "new"
                self.identifiers = new_identifiers

            logger.info(f"{class_name}: Processing {data_type} data...")

            # custom formatting - schema alignment
            logger.info(f"{class_name}: Performing custom formatting...")
            self.dfs = dfs
            self.perform_custom_formatting(temporal_column_name="month", cast_all=True)
            logger.info(f"{class_name}: Custom formatting complete.")

            # union
            logger.info(f"{class_name}: Union dataframes...")
            self.union_dataframes()
            logger.info(f"{class_name}: Union complete.")

            # de-duplicate
            logger.info(f"{class_name}: De-duplicating data...")
            self.de_duplicate(self.identifiers)
            logger.info(f"{class_name}: De-duplication complete.")

            # fix encoding
            logger.info(f"{class_name}: Fixing string encoding...")
            self.fix_str_encoding()
            logger.info(f"{class_name}: String encoding fixed.")

            # outlier treatment
            if self.delete_outliers:
                logger.info(f"{class_name}: Deleting univariate outliers...")
            else:
                logger.info(f"{class_name}: Logging univariate outliers...")
            columns = self.detect_continuous_variables(
                10,
                drop_vars=[
                    "codi_municipi",
                    "codi_provincia",
                    "estacio",
                    "codi_contaminant",
                ],
            )
            self.iqr_outlier_treatment(columns, factor=3.0, delete_outliers=self.delete_outliers)
            self.log_outliers()
            logger.info(f"{class_name}: Done.")

            # upsert
            logger.info(f"{class_name}: Upserting data to MongoDB...")
            self.upsert_formatted_db(collection_name=f"airquality_{data_type}")
            logger.info(f"{class_name}: Upsert complete.")
            self.merged_df = None
            logger.info(f"{class_name}: Processing complete for {data_type} data.")
