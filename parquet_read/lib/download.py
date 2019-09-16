#!/usr/local/bin/python3
# encoding: utf-8
"""
download.py

Created by Zach Schumacher on 2019-08-14.
Modified by Patrick Shaner on 2019-08-14.

Copyright (c) 2019 Patrick Shaner

Downloads data files from S3 file to local storage

Version 2019-08-14
    Initial file creation

Version 2019-08-14
    Updated to write to a temp directory
"""
import os
import s3fs

from multiprocessing import Process, Queue, cpu_count
from urllib3.connection import NewConnectionError


class DownloadError(Exception):
    pass


class ParquetFromS3:
    """
    Downloads all the elements in a parquet directory using multiprocessing.
        :param connection: s3fS connection to S3 storage
        :param parent_dir: directory created to store all elements
        :param uri: S3 path to download

    :: How to run::
    Initialize class with a connection created leveraging S3FS, pass in the directory you want to save the
    parquet directory to and the parquet file to download from S3

    Call property download_parquet_files, this will return a boolean of True if all files now exist on the
    local storage (successfully downloaded) or false if not
    """

    _connection = None                                                  # Connection to S3 leveraging S3FS
    _destination_path = None                                            # Directory to write files to as downloads
    _uri = None                                                         # S3 Bucket to download from
    _queue = None                                                       # Threading Queue
    _num_of_processes = None                                            # Number of process for Queue
    _files_to_download = None                                           # List of all files to download from S3

    def __init__(self, connection: s3fs, parent_dir: str, uri: str) -> None:
        """
        Downloads all the elements in a parquet directory using multiprocessing.
        :param connection: s3fS connection to S3 storage
        :param parent_dir: directory created to store all elements
        :param uri: S3 path to download
        """
        self._files_to_download = list()
        self._num_of_processes = cpu_count()
        self._queue = Queue()
        self._uri = uri

        self._set_connection_to_s3(connection)
        self._set_destination_directory(parent_dir)
        self._create_list_of_files_to_download()

    @property
    def destination_path(self):
        return self._destination_path

    @property
    def successfully_downloaded(self) -> bool:
        """
        Validates that all files expected to be downloaded actually download
        :return: bool
        """
        found_all_results = True

        for file_name in self._files_to_download:
            final_path = os.path.join(self.destination_path, file_name)
            if not os.path.exists(final_path):
                found_all_results = False
                break
            if os.path.getsize(final_path) <= 1:
                found_all_results = False
                break

        return found_all_results

    def download_files_from_s3(self) -> None:
        """
        Validates that all files located in S3 successfully downloaded to local disk
        :return: None
        """
        processes = []
        for i in range(self._num_of_processes):
            p = Process(target=self._download_single_file, args=(self._queue,))
            processes.append(p)
            p.start()

        for process in processes:
            process.join()

    def _create_list_of_files_to_download(self) -> None:
        """
        Iterates over the S3 path creating a list of all files that do not start with '_'
        :return: None
        """
        for file_name in self._connection.ls(f's3://{self._uri}'):
            if not os.path.basename(file_name).startswith('_'):
                self._queue.put(file_name)
                _, name_part = os.path.split(file_name)
                self._files_to_download.append(name_part)

    def _set_connection_to_s3(self, connection: s3fs) -> None:
        """
        Validates connection is correct type and not None, if None creates a new connection leveraging ENV Variables
        :param connection: Connection object created from env variables by caller
        :return: None
        """
        if connection:
            self._connection = connection
        else:
            key = os.getenv("S3_ACCESS", None)
            secret = os.getenv("S3_SECRET", None)

            if key and secret:
                self._connection = s3fs.S3FileSystem(key=key, secret=secret)
            else:
                raise DownloadError("Unable to make connection to S3 because required "
                                    "environment variables are not set")

    def _set_destination_directory(self, parent_dir: str) -> None:
        """
        Creates a destination directory within the parent directory
        :param parent_dir: string file path
        :return: None
        """
        if not parent_dir:
            raise DownloadError("Destination directory is not set")
        else:
            _, file_name = os.path.split(self._uri)
            self._destination_path = os.path.join(parent_dir, f"{file_name}")

            if not os.path.exists(self._destination_path):
                os.mkdir(self._destination_path)

    def _download_single_file(self, queue):
        try:
            while not queue.empty():
                path = queue.get()
                filename = os.path.basename(path)
                _target_path = os.path.join(self._destination_path, filename)

                with self._connection.open(path, 'rb') as remote_file, open(_target_path, 'wb') as local_file:
                    local_file.write(remote_file.read())
        except NewConnectionError as con:
            raise DownloadError(f"Failed to complete downloading because {con}")
