import logging
import sys

def get_logger(name: str = "glue-etl-logger") -> logging.Logger:
    """
    Creates and returns a configured logger for AWS Glue ETL jobs.
    
    :param name: Logger name (default is 'glue-etl-logger')
    :return: Configured Logger object
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger