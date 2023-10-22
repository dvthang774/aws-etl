import boto3


def create_and_run_crawler(crawl_name,delta_path,database_name):
    # Check Crawler
    glue_client = boto3.client('glue')
    
    try:
        # Check if the crawler exists in Glue
        response = glue_client.get_crawler(Name=crawler_name)
        status = "Crawler '{}' exists.".format(crawler_name)
        sms_crawler = {
                "statusCode":409,
                "sms": status
            }
            
        return sms_crawler
    # Create Crawler
    except:
        glue_client = boto3.client('glue')
        response = glue_client.create_crawler(
            Name=crawl_name,
            Role='arn:aws:iam::054804618490:role/service-role/NewS3RoleReadOnly',
            DatabaseName=database_name,
             Targets={
                    'DeltaTargets': [
                        {
                            'DeltaTables': [delta_path],
                            'WriteManifest': True
                        },
                    ]
                },
                SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
                },
                RecrawlPolicy={
                    'RecrawlBehavior': 'CRAWL_EVERYTHING'
                },
                LineageConfiguration={
                    'CrawlerLineageSettings': 'DISABLE'
                }
            )
         #  Start Crawler
        crawler_run_id = glue_client.start_crawler(
            Name=crawl_name
        )
        print(crawler_run_id)
        return {
            "statusCode": 200,
            "glue_sms": f"Created Crawler{crawl_name} and started successfully."
        }
    

# Check latest parquest file path:
def get_newest_parquet_path(bucket_name, folder_name):
    """Get the path of the newest Parquet file within the specified folder."""
    newest_path = None
    newest_last_modified = None
    
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name + '/')
    
    for item in response['Contents']:
        key = item['Key']
        if key.endswith('.parquet'):  # Filter for Parquet files
            last_modified = item['LastModified']
            
            if newest_last_modified is None or last_modified > newest_last_modified:
                newest_path = key
                newest_last_modified = last_modified
                
    return newest_path

# Run and Update Crawler
def run_crawler(crawler_name):
    glue_client = boto3.client('glue')
    glue_client.start_crawler(Name=crawler_name)
    
    return f'Update crawler {crawler_name} successfully!'
