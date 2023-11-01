from database import *
from database.database_types import *
from src.utility.utils import CommonUtility
from sqlalchemy import text

"""
export DYLD_LIBRARY_PATH=/usr/local/mysql/lib/

odbcinst -j
sudo ln -s /usr/local/etc/odbcinst.ini /etc/odbcinst.ini
sudo ln -s /usr/local/etc/odbc.ini /etc/odbc.ini  

Also had to run:
brew install openssl
Had to create new symlink for openssl1.1 

Good resource: https://docs.microsoft.com/en-us/azure/azure-sql/database/connect-query-python
"""

DB_TYPE_MAPPING = {
    MSSQL: DatabaseMsSQL,
    MYSQL: DatabaseMySQL,
    POSTGRES: DatabasePostgres,
    ORACLE: DatabaseOracle
}


class SourceDatabase:
    database = None

    def __init__(self) -> None:
        self.logger = get_logger(__name__)

        category = SECTION_SOURCE

        source_secret = CommonUtility.get_secret(secret_name=CommonUtility.read_configurations(SOURCE_SECRET_ID,
                                                                                               CONFIG_FILE,
                                                                                               section_name=category))
        schema_valid = CommonUtility.verify_secret_schema(source_secret)

        if schema_valid:
            self.logger.info("The schema for the secret is valid")

        self.db_type = CommonUtility.read_configurations(SOURCE_DATABASE_TYPE, CONFIG_FILE, section_name=category)

        self.host = source_secret.get(HOST)
        self.port = int(source_secret.get(PORT))
        self.database = source_secret.get(DATABASE_NAME)
        self.username = source_secret.get(USERNAME)
        self.password = source_secret.get(PASSWORD)

        self.db = create_database(self.db_type, self.host, self.port, self.database, self.username, self.password)

    def execute_query(self, query):
        try:
            return self.db.execute_query(text(query))
        except Exception as e:
            self.logger.error(f"An unexpected error occurred when trying to run the query: {e}")
            self.logger.error("Exiting program.")
            exit(1)


class TargetDatabase:
    def __init__(self) -> None:
        self.logger = get_logger(__name__)

        category = SECTION_TARGET
        target_secret = CommonUtility.get_secret(CommonUtility.read_configurations(TARGET_SECRET_ID, CONFIG_FILE,
                                                                                   section_name=category))
        CommonUtility.verify_secret_schema(target_secret)
        self.db_type = CommonUtility.read_configurations(TARGET_DATABASE_TYPE, CONFIG_FILE,
                                                         section_name=category)
        self.host = target_secret.get(HOST)
        self.port = int(target_secret.get(PORT))
        self.database = target_secret.get(DATABASE_NAME)
        self.username = target_secret.get(USERNAME)
        self.password = target_secret.get(PASSWORD)

        self.db = create_database(self.db_type, self.host, self.port, self.database, self.username, self.password)

    def execute_query(self, query):
        try:
            return self.db.execute_query(text(query))
        except Exception as e:
            self.logger.error(f"An unexpected error occurred when trying to run the query: {e}")
            self.logger.error("Exiting program.")
            exit(1)


def create_database(database_type, host, port, database_name, username, password):
    return DB_TYPE_MAPPING[database_type.lower()](host, port, database_name, username, password)
