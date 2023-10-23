import boto3
import time
import csv 
import json
import run_glue_job

def lambda_handler(event, context):
    # define client service
    s3 = boto3.client('s3')
    glue = boto3.client('glue')
    
    # Get the S3 bucket and object key from the event
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_object_key = event['Records'][0]['s3']['object']['key']
    print(s3_bucket, s3_object_key)
    # Log the bucket and object key for debugging
    print(f"Received event from S3 bucket: {s3_bucket}, object key: {s3_object_key}")

    # Extract the CSV file name (object key) and process it as needed
    csv_file_name = s3_object_key.split('/')[-1]
    print(csv_file_name)
    # Your processing logic goes here
    # For example, you can read the contents of the CSV file using the boto3 library:
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_object_key)
    csv_contents = response['Body'].read().decode('utf-8')

    # Now you can do something with the CSV contents
    
    
    # Now create delta table from csv using glue client
    # step1: define parameter
    csv_file_path = f"s3://mydata-landing-bucket/{s3_object_key}"
    bucket_for_bronze_layer = "delta-bronze-layer-bucket"
    delta_table_name = csv_file_name.split(".csv")[0]  # day_one
    delta_table_path = "s3://delta-bronze-layer-bucket/delta/tableA/" 
    print(delta_table_path)
    
    # step2: generate scripts
    scripts = """
from pyspark.sql import SparkSession
import json 
import csv
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder.appName("SparkSQL")
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)   

df_read_csv = spark.read.format("csv").option("header", "true").load("{}")
df_read_csv.write.format("delta").mode("append").save("{}")
enable_delta = 'ALTER TABLE delta.`{}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)'""".format(csv_file_path,delta_table_path,delta_table_path)
    
    # step3: put script to other bucket and then automatically delete after 3 three days
    final_scripts = scripts.encode("utf-8")
    
    s3 = boto3.resource(service_name = 's3',region_name = 'us-east-1',
                            aws_access_key_id = '',
                            aws_secret_access_key ='')
    
    tmp_bucket = "log-tmp-bucket"
    tmp_python_folder = "tmp/python/"
    file = delta_table_name.replace(" ","_").lower() + "_"
    datetime = time.strftime("%Y_%m_%d_%H_%M_%S")
    python_file = "%s%s%s.py"%(tmp_python_folder,file,datetime)

    s3.Bucket(tmp_bucket).put_object(Key=python_file, Body=final_scripts)
    python_file_path = "s3://" + tmp_bucket + "/" + python_file
    
    # step 4: create and run spark job in glue service
    # step 4.1 Check job is exist or not
    glue = boto3.client('glue')
    response_glue_name = glue.get_jobs()
    all_job_names = [job['Name'] for job in response_glue_name['Jobs']]
    
    # step 4.2 Create and run Glue job
    job_name = f"job_create_{delta_table_name}"
    status_glue_service = run_glue_job.create_glue_job(job_name, python_file_path)

    # Return a response
    return {
        "statusCode": 200,
        "Processed CSV file": csv_file_name,
        "Delta table path": delta_table_path,
        "CSV file path": csv_file_path,
        "Python file path": python_file_path,
        "StatusGlueService": status_glue_service
    }





