import os
import yaml
import boto3
import etl.load
import etl.extract
import utility.logger

def run_pipeline():
    with open("config.yml") as stream:
        config = yaml.safe_load(stream)

    logger = utility.logger.createLogger()

    bucket = os.environ.get(f"s3_bucket")
    client = boto3.client("s3", 
                        aws_access_key_id=os.environ.get(f"s3_access_key"),
                        aws_secret_access_key=os.environ.get(f"s3_secret_key"),
                        region_name=os.environ.get(f"s3_aws_region")
                        )


    etl.extract.extractS3(client, bucket, config['files'], logger)
    etl.load.loadAirbyte(os.environ.get(f"airbyte_server"), os.environ.get(f"airbyte_auth"), config['load'], logger)
    logger.info("Process finished successfully")

if __name__ == "__main__":
    run_pipeline()