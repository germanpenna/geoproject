import io
import requests
import utility.extract


def extractS3(
        client,
        bucket:str,
        models:dict,
        logger
        ):
    """
        Parameters
        ----------
        client : boto3 client
            The client to connect to S3
        bucket : str
            The S3 bucket name
        models : dict
            The dictionary read from the YAML file with the URLs of the files
        logger
            Logging object to write the results of the process
    """
    
    logger.info("Validating existing models")
    filesExisting = utility.extract.checkExisting(path='source', client=client, bucket=bucket, includeBlock=False)

    logger.info("Files already processed")
    for i in filesExisting:
        logger.info(i)

    try:
        for i, v in models.items():
            if i in filesExisting:
                logger.warning(f"{i} already existed in S3 Bucket")
                continue
            
            logger.info(f"Loading {i}")
            file = requests.get(models[i])
            filename, parsed_shape = utility.extract.createConvex(file)
            df = utility.extract.createDataFrame(parsed_shape)
            
            client.put_object(Bucket=bucket, Key=f"raw/{filename}.zip", Body=file.content)

            out_buffer = io.BytesIO()
            df.to_parquet(out_buffer, index=False)
            client.put_object(Bucket=bucket, Key=f"source/{filename}.parquet", Body=out_buffer.getvalue())
            logger.info(f"{i} succesfully loaded")
    except:
        raise Exception("Error: The extract process failed")
