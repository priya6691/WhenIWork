# How to run
The file Solution.py has the solution for the coding challenge.This is written in python3 and can be run using cmd python3 solution.py or via any IDEs like pycharm or IntelliJ after ensuring following python libraries are imported and available.

import pandas as pd

import validators

import boto3

import io

import os

from string import ascii_lowercase

from urllib.parse import urlparse

from botocore.exceptions import ClientError



# Assumptions
The Solution is built with following assumptions considered,
1. The csv files that should be merged will have the ascii lower case letters as their names.
2. The files will be available in public root url or in an S3 bucket and accessible.

# Input
The code accepts a public root url path or an S3 url having the files.
 If S3 url is provided, authentication details need to be added in code line 36 before running the code(s3_client = boto3.client('s3', aws_access_key_id='<aws_access_key_id>',aws_secret_access_key='<aws_secret_access_key>')
 
#ouput
The output csv file will be created in the folder where this code will be checked out/downloaded and run.
