"""
Includes classes for different database types to serve as SOURCE/DESTINATION
"""
import pandas as pd
from abc import ABC
from pandas import DataFrame
from sqlalchemy import create_engine

from logger import get_logger


class Database(ABC):
    chunk_size = 5000

    def __init__(self) -> None:
        self.logger = get_logger(__name__)
        self.engine = None
        self.database_type = None

    def execute_query(self, query):
        """
        execute_query method takes the sql query as input. It uses pandas data frame to process the data
        :param query: Query to execute in the database e.g. "select * from table_name", The
        chunk size is obtained from the property CHUNK_SIZE configured in configurations.ini file
        :return: Returns pandas dataframe
        """
        data = None
        try:
            conn = self.engine.connect()
            data = pd.read_sql(query, conn, chunksize=int(self.chunk_size))
            df = DataFrame()
            for chunk in data:
                df = pd.concat([df, chunk])
            conn.close()
            return df.to_dict('records')
        # except sqlalchemy.exc.OperationalError as e:
        except Exception as e:
            self.logger.error(f'Error occurred while executing the {self.database_type} query: {e}')
            exit(0)


class DatabaseMySQL(Database):
    chunk_size = 5000

    def __init__(self, hostname, port, database, username, password) -> None:
        super().__init__()
        self.logger = get_logger(__name__)
        url = f'mysql+pymysql://{username}:{password}@{hostname}:{port}/{database}'
        self.database_type = 'MySQL'
        self.hostname = hostname
        self.port = port
        self.database = database
        self.username = username

        try:
            self.logger.info("Trying to create the MySQL DB engine")
            self.engine = create_engine(url)
        except Exception as e:
            self.logger.error(e)
            exit(0)


class DatabaseMsSQL(Database):
    chunk_size = 5000

    def __init__(self, hostname, port, database, username, password) -> None:
        super().__init__()
        self.logger = get_logger(__name__)
        self.database_type = 'MsSQL'
        self.hostname = hostname
        self.port = port
        self.database = database
        self.username = username

        url = (f'mssql+pyodbc://{username}:{password}@{hostname}:{port}/{database}?driver=ODBC+Driver+18+for+SQL'
               f'+Server&Encrypt=yes&TrustServerCertificate=yes')
        # TLS1.2
        try:
            self.logger.info("Trying to create the MsSQL DB engine")
            self.engine = create_engine(url)
        except Exception as e:
            self.logger.error(e)
            exit()


class DatabasePostgres(Database):
    chunk_size = 5000

    def __init__(self, hostname, port, database, username, password) -> None:
        super().__init__()
        self.logger = get_logger(__name__)
        self.database_type = 'PostgreSQL'
        self.hostname = hostname
        self.port = port
        self.database = database
        self.username = username

        url = f'postgresql://{username}:{password}@{hostname}:{port}' \
              f'/{database}?sslmode=require'

        try:
            self.logger.info("Trying to create the PostgreSQL DB engine")
            self.engine = create_engine(url)
        except Exception as e:
            self.logger.error(e)
            exit()


class DatabaseOracle(Database):
    chunk_size = 5000

    def __init__(self, hostname, port, database, username, password) -> None:
        # oracle_client_path = os.path.join(os.environ.get(PYTHONPATH), 'oracle_client')
        # cx_Oracle.init_oracle_client(oracle_client_path)
        super().__init__()
        self.logger = get_logger(__name__)
        self.database_type = 'Oracle'
        self.hostname = hostname
        self.port = port
        self.database = database
        self.username = username

        url = f'oracle+cx_oracle://{username}:{password}@{hostname}:{port}/?service_name={database}'

        url = f'oracle+oracledb://{username}:{password}@{hostname}:{port}/?service_name={database}'
        try:
            self.logger.info("Trying to create the Oracle DB engine")
            self.engine = create_engine(url, max_identifier_length=128)
            self.logger.info("Created the Oracle DB engine")
        except Exception as e:
            self.logger.error(f"An error occurred during creation of Oracle DB: {e}")
            exit()
