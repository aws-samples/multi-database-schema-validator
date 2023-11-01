"""
Common utilities file
"""
import os
import json
import boto3
from logger import get_logger
from pathlib import Path
from configparser import ConfigParser
from database import CONFIG_FILE, SECRET_REGION, SECTION_REGION

from src.utility import CONFIGURATION_DIR


class CommonUtility:
    logger = get_logger(__name__)

    @staticmethod
    def read_configurations(property_name, config_file=None, section_name=None):
        value = os.environ.get(property_name, None)

        if not value:
            # Reading the value from the config file
            config_path = os.path.join(CommonUtility.get_project_root() + "/conf")
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

    @staticmethod
    def get_project_root_old() -> Path:
        return Path(__file__).parent.parent.parent

    @staticmethod
    def get_secret(secret_name):
        # Create a Secrets Manager client
        session = boto3.session.Session()

        try:
            client = session.client(
                service_name='secretsmanager',
                region_name=CommonUtility.read_configurations(SECRET_REGION, CONFIG_FILE, SECTION_REGION)
            )

            CommonUtility.logger.info(f"Trying to fetch the secret: {secret_name}")
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
            CommonUtility.logger.info("Successfully retrieved secret")
        except Exception as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            CommonUtility.logger.error("Ensure that the Secrets Manager secret has been correctly created in the "
                                       "mentioned region.")
            CommonUtility.logger.error(e)
            exit(0)

        # Decrypts secret using the associated KMS key.
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret

    @staticmethod
    def verify_secret_schema(secret_dict):
        keys = ["host", "port", "username", "password", "database_name"]

        for key in keys:
            if key not in secret_dict:
                CommonUtility.logger.error(f"Key: {key} is not present in the secret. Update the secret")
                exit(0)

        return True

    @staticmethod
    def get_project_root():
        # Get the directory of the file containing the function
        function_dir = os.path.dirname(os.path.abspath(os.path.abspath(__file__)))
        root_dir = os.path.dirname(os.path.dirname(function_dir))

        return root_dir
