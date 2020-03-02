import requests
import json
import boto3
import pandas as pd
import tempfile
import datetime
import io
import botocore






def lambda_handler(event, context):
 
    sourceBucket = "willwrightbasketballdata"
    destBucketName = "wwrightmovingpractice"

    client = boto3.client("s3")
    #bucket = conn.get_bucket(bucketName)
    


    #writes the json to a temp file
    with tempfile.TemporaryFile(mode = 'w', delete = False) as temp_file:   
        
        #Gets object from S3 Bucket
        callData = client.get_object(Bucket = sourceBucket, Key = 'example_call_data.csv')
        yearData = client.get_object(Bucket= sourceBucket, Key = 'test_year_merge.csv')

        #creates Dataframes for callData and yearData
        callDF = pd.read_csv(io.BytesIO(callData['Body'].read()), encoding = 'utf-8')
        yearDF = pd.read_csv(io.BytesIO(yearData['Body'].read()), encoding = 'utf-8')
        
        #merges Dataframes together
        combinedDF = pd.merge(callDF, yearDF, on = "Result")
        combinedDF = combinedDF.to_csv()
        
        #writes to a new temp file
        temp_file.write(combinedDF)
        temp_file.seek(0)
        
        
        #uploads to willwrightbasketballdata bucket in AWS
        client.upload_file(Filename = temp_file.name, Bucket = destBucketName, Key = "Combined_Call_Data")
        #AKIAIFPB73P25Y4JWI3A
        temp_file.close

        try:
            #checks if todays object exists in the destination bucket
            client.head_object(Bucket = destBucketName, Key = "Combined_Call_Data")
            
        except botocore.exceptions.ClientError as e:
            #if the object doesn't exist, it excepts and adds it to a log
            if e.response['Error']['Code'] == "404":
                print("add to log")

        else:
            #if the file does exist and was created correct, it deletes the original source files
                client.delete_object(Bucket = sourceBucket, Key = 'example_call_data.csv')
                client.delete_object(Bucket = sourceBucket, Key = 'test_year_merge.csv')



if __name__ == "__main__":
    lambda_handler(None, None)



  

