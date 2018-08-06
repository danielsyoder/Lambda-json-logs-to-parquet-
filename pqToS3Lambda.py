import warnings
warnings.filterwarnings("ignore") #to ignore numpy/pandas warning
from StringIO import StringIO
import json
import uuid
from fastparquet import write
import boto3
import gzip
#import pandas as pd #(for further DF transformation)
#import numpy as np
from pandas.io.json import json_normalize

s3 = boto3.client('s3')
outputBucket = 'outputS3Bucket'
outputKeyspace = 'outputFolderKey/'

def writePQtoS3(df):
    #Write compressed parquet file to S3
    #Randomized output name as function can combine several logs
    #TODO Potentially add key to map /hour/ to folder partitions
    bucket = outputBucket
    name = str(uuid.uuid4())[:20]
    filename = 'log_'+name+'.parquet.gzip'
    key = outputKeyspace+filename
    write('/tmp/'+filename, df,compression='GZIP') #fastparquet supports SNAPPY
    pqFile = '/tmp/'+filename
    s3.upload_file(pqFile,bucket, Key=key)
    print('uploaded: ',filename)

def transform_DF(recordList):
        #Flatten nested JSON and cast list of dicts to pandas DF
        #Set compound column separator as '_' for Athena compatability
        workingDF = json_normalize(data=recordList, sep='_')
        writePQtoS3(workingDF)
        
def lambda_handler(event, context):
    #Transfrom json.gz logs to parquet. Logs records are lines of json.
    #Single Lambda could ingest 2-3 10mb log files.
    
    incomingRecords = []
    
    for record in event['Records']:
        #read SQS Message to get new S3 log objects
        recDict = json.loads(record['body'])
        bucket = recDict['Records'][0]['s3']['bucket']['name']
        key = recDict['Records'][0]['s3']['object']['key']

        try:
            #download/open json.gz, load json as dict, then add to record list
            response = s3.get_object(Bucket=bucket, Key=key)
            data = response['Body'].read()
            unzipped = gzip.GzipFile(fileobj=StringIO(data)).read()
            lines = unzipped.split('\n')
            linesJson = [json.loads(line) for line in lines[:-1]] #modify depending on log files (last line was empty)
            incomingRecords = incomingRecords[:] + linesJson[:] 

        except Exception as e:
            print('Error with: {} in bucket {}.'.format(key, bucket))
            raise e
            
    transform_DF(incomingRecords)


            
