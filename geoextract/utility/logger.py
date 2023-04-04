import logging

def createLogger():
    logger = logging.getLogger()
    
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    log_format = '[%(levelname)s][%(asctime)s]: %(message)s'
    console_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(console_handler)

    logger.info("Connecting to the bucket")

    return logger
