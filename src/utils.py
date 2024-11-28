import logging


# setup logging
def setup_logging():
    logging.basicConfig(filenme='logs.log', 
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(filename)s - %(lineno)d:  %(message)s'
    )

