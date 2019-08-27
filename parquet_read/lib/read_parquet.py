#!/usr/local/bin/python3
# encoding: utf-8
"""
read_parquet.py

Modified by Patrick Shaner on 2019-08-14.

Copyright (c) 2019 Patrick Shaner

Reads a parquet file from local storage and converts to JSON files

Version 2019-08-14
    Initial file creation

Version 2019-08-14
    Updated to write to a temp directory
"""
import os
import shutil

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


class ReadParquetError(Exception):
    pass


class ReadParquet(object):
    """
    Reads a parquet directory created from Spark and converts into a directory of JSON files.

    ::Usage::
    tmp_file = "~/Downloads/TempDF.parquet"
    tmp = ReadParquet(tmp_file)
    tmp.process_parquet_directory()

    Creates a temporary directory containing JSON files.
    returns by caller

    tmp.json_dir
    """
    _par_dir = None                         # Directory created as part of the Parquet process
    _spark = None                           # Spark session with locally initialized
    _sql_context = None                     # Spark Sql session created from the spark session
    _data_frame = None                      # Data frame created from reading the parquet file with the SQL session
    _json_dir = None                        # Temporary directory for Spark to write JSON data from the data frame

    def __init__(self, parquet_dir: str, parent_dir: str):
        """
        Initialize the class, setting the directory to parquet_read a parquet data from.
        :param parquet_dir: path to parquet directory
        :param parent_dir: directory to write json data to
        """
        self._set_parquet_dir(parquet_dir)
        self._set_tmp_dir(parent_dir)
        self._create_spark_session()
        self._create_sql_context_and_data_frame()

    @property
    def json_directory(self) -> str:
        """
        Returns self._json_dir or the temporary directory created to contain the JSON files
        :return: self_json_dir
        """
        return self._json_dir

    @property
    def data_frame(self):
        return self._data_frame

    def read_directory_generate_raw_jsons(self, with_nulls=False) -> None:
        """
        Main call point for class, reads the parquet file and generates the data
        :param with_nulls: Will replace null values with ''
        :return: None
        """
        if with_nulls:
            # adds "" for null values in data frame
            self._data_frame.na.fill("").write.json(self._json_dir)
        else:
            self._data_frame.write.json(self._json_dir)

    def _create_spark_session(self) -> None:
        """
        Ensures all the environment variables are set as needed. Creates the spark connection
        :return:
        """
        self._spark = SparkSession.builder.master('local').appName('myAppName').config(
            'spark.executor.memory', '10g').config("spark.cores.max", "4").config(
            'spark.driver.maxResultSize', '10g').config(
            'spark.driver.memory', '10g').config('spark.debug.maxToStringFields', "500000").getOrCreate()

    def _create_sql_context_and_data_frame(self) -> None:
        """
        Creates the sql context for Spark
        :return: None
        """
        sc = self._spark.sparkContext
        self._sql_context = SQLContext(sc)
        self._data_frame = self._sql_context.read.parquet(self._par_dir)

    def _set_parquet_dir(self, par_dir: str) -> None:
        """
        Sets class variable for _par_dir verifying that exist and is directory
        :param par_dir: string to parquet directory, passed from the init
        :return: None
        """
        if os.path.exists(par_dir) and os.path.isdir(par_dir):
            self._par_dir = par_dir
            if "_spark_metadata" in os.listdir(self._par_dir):
                shutil.rmtree(os.path.join(self._par_dir, "_spark_metadata"))
        else:
            raise ReadParquetError(f"{par_dir} does not exist or is not a directory")

    def _set_tmp_dir(self, parent_dir: str) -> None:
        """
        Used to create the destination directory, where tmp JSON's will be written to.
        If directory already exists then deletes the directory.
        :return: None
        """
        full_path = os.path.join(parent_dir, "JSON_DATA")

        if os.path.exists(full_path):
            shutil.rmtree(full_path)

        self._json_dir = full_path
