#!/usr/local/bin/python3
# encoding: utf-8
"""
reader.py

Created by Patrick Shaner on 2019-08-14.

Copyright (c) 2019 Patrick Shaner

Reads parquet data from S3, saving data to json files in local file system

Version 2019-08-14
    Initial file creation
"""

import os
import s3fs
import tempfile
import datetime

from parquet_read.lib import download, read_parquet

__all__ = ["Connection"]


class S3ConnectionError(Exception):
    pass


class Connection:
    """
    Makes a connection to S3 and reads a parquet directory converting
    contents into JSON's located on the local storage

        :param uri: Path within the bucket to for parquet data
        :param bucket: Bucket to search look in S3 for can be parquet_read from env variable "S3_BUCKET"
        :param access_key: Access key to connect to S3 parquet_read from env variable "S3_ACCESS"
        :param secret_key: Secret key to connect to S3 parquet_read from env variable "S3_SECRET"
        :param destination_dir: Directory to write JSON's to, default is <date_time> located in a temp directory

    :::How to use::
    Initialize class recommend setting the following environment variables for better security
        - S3_ACCESS
        - S3_SECRET
        - S3_BUCKET
    Once class has been initialized call method download_and_convert_to_json convert the parquet data into JSON data

    ::Example::
    path = "agg_files/historical/DocObject7.parquet"
    tmp = Connection(path)
    tmp.download_and_convert_to_json()
    """

    _destination_dir = None                                 # Local directory to write JSON files to
    _bucket = None                                          # S3 bucket to look search directory for
    _uri = None                                             # Path to parquet_read from S3
    _s3_connection = None                                   # Connection to S3 leveraging Boto3
    _download_process = None                                # Download object
    _json_process = None                                    # Parquet reader object
    _full_download_path = None                              # S3 Bucket and URI combined used in S3FS

    def __init__(self, uri: str, bucket=None, access_key=None, secret_key=None, destination_dir=None) -> None:
        """
        Initialize object
        :param uri:  Path within the bucket to for parquet data
        :param bucket: Bucket to search look in S3 for can be parquet_read from env variable "S3_BUCKET"
        :param access_key: Access key to connect to S3 parquet_read from env variable "S3_ACCESS"
        :param secret_key: Secret key to connect to S3 parquet_read from env variable "S3_SECRET"
        :param destination_dir: Directory to write JSON's to, default is <date_time>_JSON located in this directory
        """
        self._set_uri_path(uri)
        self._set_bucket(bucket)
        self._set_s3_access_key(access_key)
        self._set_s3_secret_key(secret_key)
        self._set_json_destination_directory(destination_dir)
        self._create_connection()
        self._full_download_path = os.path.join(self._bucket, self._uri)

    @property
    def parent_destination_path(self):
        """
        :return: self,_destination
        """
        return self._destination_dir

    @property
    def parquet_destination(self):
        return self._download_process.destination_path

    @property
    def json_destination(self):
        return self._json_process.json_directory

    @property
    def parquet_data_frame(self):
        return self._json_process.data_frame

    @property
    def test_connection(self) -> bool:
        """
        Validates all setting and that path is reachable in S3 before starting to download
        :return: boolean, True for good connection False for false connection
        """
        path_part, _ = os.path.split(self._uri)
        s3_path = os.path.join(self._bucket, path_part)
        return self._s3_connection.exists(f's3://{s3_path}')

    def download_and_convert_to_json(self) -> None:
        """
        Downloads the parquet files and then leverages pyspark to create JSON files
        :return: None
        """
        self._create_download_obj_and_download_file()
        if self._download_process.successfully_downloaded:
            self._json_process = read_parquet.ReadParquet(self._download_process.destination_path,
                                                          self.parent_destination_path)
            self._json_process.read_directory_generate_raw_jsons()
        else:
            raise S3ConnectionError(f"Unable to download convert downloaded files to JSON because files "
                                    f"did not successfully download all files")

    def download_and_get_data_frame(self) -> None:
        """
        Downloads the parquet files and then leverages pyspark to create JSON files
        :return: None
        """
        self._create_download_obj_and_download_file()

        if self._download_process.successfully_downloaded:
            self._json_process = read_parquet.ReadParquet(self._download_process.destination_path,
                                                          self.parent_destination_path)

        else:
            raise S3ConnectionError(f"Unable to create data frame because files "
                                    f"did not successfully download all files")

    def _create_connection(self):
        """
        Makes the S3 connection leveraging boto3
        :return:
        """
        key = os.getenv("S3_ACCESS", None)
        secret = os.getenv("S3_SECRET", None)
        java_home = os.getenv("JAVA_HOME", None)

        if not java_home:
            raise S3ConnectionError("JAVA_HOME env variable is not set this will cause connection to fail")

        if key and secret:
            self._s3_connection = s3fs.S3FileSystem(key=key, secret=secret)
        else:
            raise S3ConnectionError("Unable to make connection to S3 because required "
                                    "environment variables are not set")

    def _set_uri_path(self, uri) -> None:
        """
        Sets the URI path
        :param uri: String path to connect look for parquet bucket from
        :return: None
        """
        if isinstance(uri, str):
            if uri != "" or uri is not None:
                self._uri = uri
            else:
                raise ValueError(f"Unable to assign {uri}")
        else:
            raise ValueError("Unable to set URI path because value is not a string")

    def _set_bucket(self, bucket) -> None:
        """
        Sets the s3 bucket to search environment variable S3_BUCKET first then if not set looks in function argument
        :param bucket: Optional, string for S3 bucket Name
        :return: None
        """
        bucket_name = os.getenv("S3_BUCKET", None)
        if not bucket_name:
            print("Recommend using environment variable S3_BUCKET")

            if isinstance(bucket, str):
                if bucket != "" or bucket is not None:
                    self._bucket = bucket
                else:
                    raise ValueError(f"Unable to assign {bucket}")
            else:
                raise ValueError("Unable to set S3 Bucket value because S3_BUCKET is not set")
        else:
            self._bucket = bucket_name

    def _set_json_destination_directory(self, destination_dir) -> None:
        """
        Sets the destination path based on method variable, if not set then creates a temp directory with path
        :param destination_dir: Path to write files to, if not set then will create a directory with date_time_json path
        :return: None
        """
        if not destination_dir:
            now = datetime.datetime.now()
            dir_time_stamp = now.strftime("%m-%d-%Y_%H-%M-%S")
            self._destination_dir = tempfile.mkdtemp(f"_{dir_time_stamp}")
        else:
            if not os.path.exists(destination_dir):
                os.makedirs(destination_dir, exist_ok=True)
            self._destination_dir = destination_dir

    def _create_download_obj_and_download_file(self) -> None:
        """
        Creates self._download process and downloads the file from S3
        :return: None
        """
        total_tries = 3
        if self.test_connection:
            while total_tries != 0:
                try:
                    self._download_process = download.ParquetFromS3(self._s3_connection, self.parent_destination_path,
                                                                    self._full_download_path)
                    self._download_process.download_files_from_s3()
                    break
                except download.DownloadError:
                    self._download_process.download_files_from_s3()
                    total_tries -= 1

            if total_tries == 0:
                raise S3ConnectionError(f"Unable to download {self._full_download_path} after attempting 3 times")
        else:
            raise S3ConnectionError(f"Unable to download {self._full_download_path} because "
                                    f"connection to S3 is not valid")

    @staticmethod
    def _set_s3_access_key(access_key) -> None:
        """
        Sets the s3 access key search environment variable S3_ACCESS first then if not set looks in function argument
        :param access_key: Optional, string for S3 access key
        :return: None
        """
        access_string = os.getenv("S3_ACCESS", None)
        if not access_string:
            print("Recommend using environment variable S3_BUCKET")

            if isinstance(access_key, str):
                if access_key != "" or access_key is not None:
                    os.environ["S3_ACCESS"] = access_key
                else:
                    raise ValueError(f"Unable to assign {access_key}")
            else:
                raise ValueError("Unable to set S3 access_key value because value is not a string")

    @staticmethod
    def _set_s3_secret_key(secret_key) -> None:
        """
        Sets the s3 secret key search environment variable S3_SECRET first then if not set looks in function argument
        :param secret_key: Optional, string for S3 secret key
        :return: None
        """
        secret_string = os.getenv("S3_SECRET", None)
        if not secret_string:
            print("Recommend using environment variable S3_BUCKET")

            if not isinstance(secret_key, str):
                if secret_key != "" or secret_key is not None:
                    os.environ["S3_SECRET"] = secret_key
                else:
                    raise ValueError(f"Unable to assign {secret_key}")
            else:
                raise ValueError("Unable to set S3 secret_key value because value is not a string")
