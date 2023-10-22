
# def create_glue_job(glue, job_name, python_file_path):
#     response = glue.get_jobs()
#     all_job_names = [job['Name'] for job in response['Jobs']]

#     if job_name not in all_job_names:
#         response_create_job = glue.create_job(
#             Name=job_name,
#             Description='',
#             Role='arn:aws:iam::054804618490:role/glue_role'
#             ExecutionProperty={'MaxConcurrentRuns': 2},
#             Command={
#                 'Name': 'glueetl',
#                 'ScriptLocation': python_file_path,
#                 'PythonVersion': '3'
#             },
#             DefaultArguments={
#                 '--additional-python-modules': 's3://aws-delta-demo/libraries/openpyxl-3.1.2-py2.py3-none-any.whl',
#                 '--datalake-formats': 'delta',
#                 '--extra-jars': 's3://aws-delta-demo/libraries/delta-core_2.12-2.1.0.jar'
#             },
#             MaxRetries=0,
#             GlueVersion='4.0',
#             NumberOfWorkers=2,
#             Timeout=10,
#             WorkerType='G.1X'
#         )

#         job_run_id = glue.start_job_run(JobName=job_name)['JobRunId']
#         return {
#             "statusCode": 200,
#             "glue_sms": f"Created Spark job {job_name} and started successfully.",
#             "delta_table_sms": "Delta table is being created; it can take about 3 minutes."
#         }
#     else:
#         return {
#             "statusCode": 404,
#             "glue_sms": f"Cannot create Spark job {job_name} because it already exists."
#         }


import boto3
glue = boto3.client('glue')

def create_glue_job(job_name, python_file_path):
    """Creates a Glue job.

    Args:
        job_name: The name of the job.
        python_file_path: The path to the Python script that the job will run.

    Returns:
        A dictionary containing the status code and job run ID.
    """

    # Get the list of all job names, if it's not already cached.
    cached_job_names = []
    if not cached_job_names:
        response = glue.get_jobs()
        cached_job_names = [job['Name'] for job in response['Jobs']]

    # Check if the job already exists.
    if job_name in cached_job_names:
        return {
            "statusCode": 404,
            "glue_sms": f"Cannot create Spark job {job_name} because it already exists."
        }

    # Create the job creation parameters.
    job_creation_parameters = {
        'Name': job_name,
        'Description': '',
        'Role': 'arn:aws:iam::054804618490:role/glue_role',
        'ExecutionProperty': {
            'MaxConcurrentRuns': 2
        },
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': python_file_path,
            'PythonVersion': '3'
        },
        'DefaultArguments': {
            '--additional-python-modules': 's3://aws-delta-demo/libraries/openpyxl-3.1.2-py2.py3-none-any.whl',
            '--datalake-formats': 'delta',
            '--extra-jars': 's3://aws-delta-demo/libraries/delta-core_2.12-2.1.0.jar'
        },
        'MaxRetries': 0,
        'GlueVersion': '4.0',
        'NumberOfWorkers': 2,
        'Timeout': 10,
        'WorkerType': 'G.1X'
    }

    # Create the job.
    response_create_job = glue.create_job(**job_creation_parameters)

    # Start the job run.
    job_run_id = glue.start_job_run(JobName=job_name)['JobRunId']

    # Return the status code and job run ID.
    return {
        "statusCode": 200,
        "glue_sms": f"Created Spark job {job_name} and started successfully.",
        "delta_table_sms": "Delta table is being created; it can take about 3 minutes."
    }