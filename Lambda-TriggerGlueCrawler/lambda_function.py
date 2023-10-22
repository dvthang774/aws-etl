import boto3
import time
import csv 
import json
from create_glue_crawler import create_and_run_crawler, get_newest_parquet_path, run_crawler

# define client service
s3 = boto3.client('s3')
glue = boto3.client('glue')
database_name = 'glue_demo_athena'   
delta_path = 's3://delta-bronze-layer-bucket/delta/tableA/'

def lambda_handler(event, context):
     # Get the S3 bucket and object key from the event
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_object_key = event['Records'][0]['s3']['object']['key']
    print(s3_bucket, s3_object_key)
    # Log the bucket and object key for debugging
    print(f"Received event from S3 bucket: {s3_bucket}, object key: {s3_object_key}")
    
        # Extract table name
    s3_object_key = event['Records'][0]['s3']['object']['key']
    parts = s3_object_key.split('/')
    crawler_name = "crawler_"+parts[-1]
    
    """List all crawler and check  exist"""
    # session = boto3.session.Session()
    crawler_details = glue.list_crawlers()
    list_all_crawler = []
    print(crawler_details['CrawlerNames'])
    
    elements = s3_object_key.split("/")
    print('elements:',elements)
    
    if event['Records'][0]['eventName'] == 'ObjectCreated:Put':
        if not list_all_crawler:
            for crawl in crawler_details['CrawlerNames']:
                list_all_crawler.append(crawl)
        if crawler_name in list_all_crawler:
            print( {
                "statusCode": 404,
                "glue_sms": f"Cannot create Crawler  {crawler_name} because it already exists."
            })
            
            response = run_crawler(crawler_name)
    
        # Create Crawler
        
        # response = glue.update_table(
        #             DatabaseName=DatabaseName,
        #             TableInput={
        #                 'Name': 'glue_demo_athena',
        #                 'Description': 'Table Updated',
        #             }
        #     )
            return {
                "statusCode": 200,
                "Glue_sms":f"Crawler {crawler_name} was successfully"
            }
        else:
            response = create_and_run_crawler(crawler_name,delta_path,database_name)
            return {
                "statusCode": 200,
                "Glue_sms":f"Crawler {crawler_name} was successfully"
            }
     # if event name = ObjectCreated:Put
     
    # if event['Records'][0]['eventName'] == 'ObjectCreated:Put':
    #     # update crawler
    #     if '.parquet' in s3_object_key:
    #         athena_client = boto3.client('athena')
    #         response = run_crawler(crawler_name)
    #         print('response:',response)
    #         return response
    #     elif len(elements) == 4:
    #         # suffixes_delta = object_key.split("_delta_log_%24folder%24")[0] # delta/tien3/customer_profile/
    #         # delta_path = f"s3://{bucket_name}/{suffixes_delta}"
    #         # print("delta_path:", delta_path)
            
    #         response = create_and_run_crawler(crawler_name,database_name,delta_path)
    #         print('response:',response)
    #         return response
        