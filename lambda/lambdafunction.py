import json
import boto3

def lambda_handler(event, context):

    try:
        # Extract file details
        file_name = event["Records"][0]["s3"]["object"]["key"]
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]

        print(f"File Name: {file_name}")
        print(f"Bucket Name: {bucket_name}")

        glue = boto3.client("glue")

        response = glue.start_job_run(
            JobName="glueexample",
            Arguments={
                "--VAL1": file_name,
                "--VAL2": bucket_name
            }
        )

        print("Glue Job Started:", response["JobRunId"])

        return {
            "statusCode": 200,
            "body": json.dumps("Lambda triggered Glue successfully!")
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps(f"Failed to trigger Glue job: {str(e)}")
        }