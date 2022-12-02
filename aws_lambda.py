import json
import boto3

#Extract filename and bucket name that is triggering lambda
def lambda_handler(event, context):
    file_name = event['Records'][0]['s3']['object']['key']
    bucketName=event['Records'][0]['s3']['bucket']['name']
    print("File Name : ",file_name)
    print("Bucket Name : ",bucketName)

    #Create client for glue, as lambda triggers glue.
    #Run glue job
    glue=boto3.client('glue');
    response = glue.start_job_run(JobName = "jsonbigdatapipeline", Arguments={"--FILENAME":file_name,"--BUCKETNAME":bucketName})
    print("Lambda Invoke ")