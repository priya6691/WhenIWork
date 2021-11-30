
import pandas as pd
import validators
import boto3
import io
import os
from string import ascii_lowercase
from urllib.parse import urlparse
from botocore.exceptions import ClientError

def merge_files_from_url(path):

    master = pd.DataFrame()

    ## Iterate through ascii lowercase and generate file paths
    try:
        for c in ascii_lowercase:
            file = path + c + ".csv"
            master = master.append(pd.read_csv(file))
    except OSError as err:
        print("OS error: {0}".format(err))

    if master.empty:
        ## When no files available to process create an empty file and return
        master.to_csv('MergedFromUrl.csv')
        print('Empty file created in the path', os.getcwd(), 'File name : MergedFromUrl.csv')
        return ()

    transposed = master.pivot(index='user_id', columns='path', values='length').reset_index()

    #Replacing NA with 0
    transposed = transposed.fillna(0)
    #print(transposed.to_string(index=False))
    transposed.to_csv('MergedFromUrl.csv',index=False)
    print('Files are processed and merged file is successfully created in the path :',os.getcwd(), 'File name : MergedFromUrl.csv')

def merge_files_from_s3(bucket):

    # Please provide a valid access key id and key for authentication
    s3_client = boto3.client('s3', aws_access_key_id='<aws_access_key_id>',
                             aws_secret_access_key='<aws_access_key_id>')

    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError:
        print("The bucket does not exist or cannot be accessed. Please try with a valid bucket")

    result = s3_client.list_objects_v2(Bucket=bucket, Delimiter='/*')
    master = pd.DataFrame()

    try:
        for r in result["Contents"]:
            response = s3_client.get_object(Bucket=bucket, Key=r["Key"])
            master = master.append(pd.read_csv(io.BytesIO(response['Body'].read())))
    except BaseException as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise

    if master.empty:
        ## When no files available to process create an empty file and return
        master.to_csv('MergedFromS3.csv')
        print('Empty file created in the path', os.getcwd(), 'File name : MergedFromS3.csv')
        return ()

    transposed = master.pivot(index='user_id', columns='path', values='length').reset_index()
    transposed = transposed.fillna(0)
    #print(transposed.to_string(index=False))
    transposed.to_csv('MergedFromS3.csv', index=False)
    print('File is successfully created in the path :', os.getcwd(), 'File name : MergedFromS3.csv')


if __name__ == '__main__':
    # path = r'https://public.wiwdata.com/engineering-challenge/data/'
    path = input("Enter the public root url or an S3 url:")

    # Check if path is provided
    if path == 'undefined' or path == '':
        raise Exception('Input path is empty. Please try again with a valid path')

    if 's3' not in path and validators.url(path) == True:
        merge_files_from_url(path)
    elif 's3' in path and validators.url(path) == True:
        o = urlparse(path, allow_fragments=False)
        bucket = o.netloc.split('.')
        print('Thanks for providing an S3 url. Extracted Bucket name is : ',bucket[0], 'Processing files ..')
        merge_files_from_s3(bucket[0])
    elif 's3' in path:
        o = urlparse(path, allow_fragments=False)
        bucket = o.netloc.split('.')
        print('Thanks for providing an S3 url. Extracted Bucket name is : ', bucket[0], 'Processing files ..')
        merge_files_from_s3(bucket[0])
    else:
        print('Please enter a valid url or S3 Bucket name')




