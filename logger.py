import os
import logging
from configparser import ConfigParser

from database import CONFIG_FILE
from src import *
from time import strftime, gmtime

from src.utility import CONFIGURATION_DIR


def get_project_root():
    # Get the directory of the file containing the function
    root_dir = os.path.dirname(os.path.abspath(os.path.abspath(__file__)))
    return root_dir


def read_configurations(property_name, config_file=None, section_name=None):
    value = os.environ.get(property_name, None)

    if not value:
        # Reading the value from the config file
        config_path = os.path.join(get_project_root() + "/conf")
        if config_path:
            path = config_path
        else:
            path = os.path.join(os.getcwd(), CONFIGURATION_DIR)

        path = os.path.join(path, config_file)
        config = ConfigParser()
        config.read(path)
        if section_name in config.sections():
            value = config[section_name].get(property_name)
        else:
            value = ''
    return value


python_path = get_project_root()
LOG_DIR = os.path.join(os.path.normpath(python_path) if python_path else '', 'logs')

if not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)

LOG_FILE_NAME = os.path.join(LOG_DIR, LOG_FILE_FORMAT.format(strftime('%Y%m%d_%H%M%S', gmtime())))
FORMAT = logging.Formatter(LOGGING_FORMAT)
FILE_HANDLER = logging.FileHandler(LOG_FILE_NAME)
FILE_HANDLER.setFormatter(FORMAT)
stream_handler = logging.StreamHandler()


def get_logger(name, level=None):
    level = level if level else read_configurations(DEBUG_LEVEL, CONFIG_FILE, LOGGING)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(FILE_HANDLER)
    logger.addHandler(stream_handler)
    return logger
