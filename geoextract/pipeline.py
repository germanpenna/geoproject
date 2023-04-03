import os
import yaml
import boto3
import logging
import etl.extract
import etl.load
from graphlib import TopologicalSorter
from airbyte.airbyte import AirbyteConnection

def run_pipeline():
    with open("config.yml") as stream:
        config = yaml.safe_load(stream)

    logger = logging.getLogger()
    
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    log_format = '[%(levelname)s][%(asctime)s]: %(message)s'
    console_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(console_handler)

    logger.info("Connecting to the bucket")


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