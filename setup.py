#!/usr/local/bin/python3
# encoding: utf-8
"""
setup.py

Created by Patrick Shaner on 2019-08-14.

Copyright (c) 2019 Patrick Shaner

Setup process for S3 Parquet reader

Version 2019-08-14
    Initial file creation
"""
from setuptools import setup


with open('requirements.txt') as f:
    required = f.read().splitlines()


setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='parquet_read',
    version='1.0',
    url='https://github.com/pshaner982/s3_parquet_reader',
    author="Patrick Shaner and Zach Schumacher",
    author_email='patrick.shaner@questanalytics',
    # Needed to actually package something
    packages=['parquet_read', "parquet_read.lib"],
    # Needed for dependencies
    install_requires=[required],
    # The license can be anything you like
    license='MIT',
    description='A S3 Utility for reading parquet files and convert to JSONs',
    # We will also need a readme eventually (there will be a warning)
    long_description=open('README.md').read(),
)
