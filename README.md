# Extract-Json-files-into-S3 And Raise exception when fails


from __future__ import print_function
import sys
import tarfile
import json
import boto3
import botocore
import io
from io import BytesIO
from io import StringIO
import os
import stat
import math
import time
from datetime import date


s3_client = boto3.client('s3')
#s3_client = boto3.resource('s3')

today = date.today()
# dd/mm/YY
dateKey = today.strftime("%Y%m%d")
print("dateKey =", dateKey)
tarFileName="mn-prod_"+dateKey+"_011001.tgz"
bucket_name = "cp-prod-"
print("fileName =", tarFileName)


    
def extractJsons():
    try:
        input_tar_file = s3_client.get_object(Bucket = bucket_name, Key = tarFileName)
        input_tar_content = input_tar_file['Body'].read()
    
        with tarfile.open(fileobj = BytesIO(input_tar_content)) as tar:
            for json_resource in tar:
                if (json_resource.isfile()):
                    inner_file_bytes = tar.extractfile(json_resource).read()
                    print(json_resource.name)
                    s3_client.upload_fileobj(BytesIO(inner_file_bytes), Bucket = bucket_name, Key = 'sftp_json/mndb/'+json_resource.name) 
                    s3_client.delete_object(Bucket = bucket_name, Key = tarFileName)
    except Exception as e:
        message = {"An error occurred while extracting targz file"}
        client = boto3.client('sns', region_name='eu-west-1')
        response = client.publish(
            TargetArn="arn:aws:sns:eu-west-1:070:gluetest",
            Message=json.dumps({'default': json.dumps(message)}),
            Subject='Extraction Failed',
            MessageStructure='json'
        )

print ("##Extract Job process START##")
extractJsons()
print ("##Extract Job process END##")
    
