# Multi Database Schema Validator

### Overview

For a database migration, schema validation plays a vital role. This Python based validation tool helps in finding the gaps between source and target database objects like Tables, Views, Indexes, Keys, Stored procedures, Functions, Triggers and Sequences with encrypted connection. It generates an output summary and a detailed report in formats such as Microsoft Excel (xlsx) and html. The report explains the object level match percentage (%) per database, per schema and list of missing objects at target referring to the source. 

### Flow Diagram
**The following diagram explains the high level flow of the tool considering on-premise as source and AWS Cloud as target**
 ![Flow Diagram](/Multi-Database-Schema-Validator-Flow.png "Flow Diagram")

Note: Tool can be deployed either on source or target system following the pre-requisites.

### Pre-requisites

* [Python 3.10.x](https://www.python.org/downloads/)
* [ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16#download-for-windows)
* [Connectivity](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/CHAP_CommonTasks.Connect.html) to the source and target databases from the host machine.
* "Select" permissions on metadata tables or views for the user executing queries on source and target databases.
* [AWS CLI](https://aws.amazon.com/cli/) configured in the host machine.

Note: Make sure the user has admin privileges to install the pre-requisites.

### Operating System

* Currently supported on Windows and macOs.

Note: To fix few installation errors on macOs, please go through the following steps.
1. To fix the error - "ERROR: Failed building wheel for pymssql", uninstall pymssql using the following command

```text
pip3 uninstall pymssql
```

2. Re-install pymssql

```text
pip3 install pymssql
```

3. To fix the error - "Error occurred while executing the MsSQL query: (pyodbc.Error) ('01000', "[01000] [unixODBC][Driver Manager]Can't open lib 'ODBC Driver 18 for SQL Server' : file not found (0) (SQLDriverConnect)")" , install the [ODBC driver for mac](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16) Operating system

### Databases Supported

* SQL Server
* Oracle
* PostgreSQL
* Mysql

Note: For Oracle to Postgres validation, please refer the post [Schema and code validator for Oracle to Amazon RDS for PostgreSQL or Amazon Aurora PostgreSQL migration](https://aws.amazon.com/blogs/database/schema-and-code-validator-for-oracle-to-amazon-rds-for-postgresql-or-amazon-aurora-postgresql-migration/)

### Tool Set-up

Download the code from [git repository](https://gitlab.aws.dev/dbmigautomations/multi-database-schema-validator) to the host machine. Open the command prompt, and run one of the following commands to check the Python version installed

```text
python --version

python3 --version
```
 
Based on the Python version installed on the host machine, navigate to the code downloaded directory, run one of the following commands with appropriate version requirements file.

For example if you have Python 3.10.x installed on your host machine, run the following command

```text
python -m pip install -r requirements3.10.txt

python3 -m pip install -r requirements3.10.txt
```

#### Setting up AWS Secret Manager secrets for Source and Target database details:

The details for the databases such as host names, username, password, etc. need to be specified in separate AWS Secret Manager secrets which provide more security for your credentials.

The following steps explain how to create a secret:

1. Open the Secrets Manager [Console](https://us-east-1.console.aws.amazon.com/secretsmanager/home?region=us-east-1)

2. Ensure that you are in the correct region

3. Click on **Store a new secret**

4. Select **Other type of secret**

5. Under Plaintext specify your database details in the following manner:

    ```json
    {
        "username": "<username>",
        "password": "<password>",
        "host": "<database_hostname>",
        "port": "<port>",
        "database_name": "<database_name>"
    }
    ```

6. Select the Encryption key to be used for secret encryption. Ensure that the role/user used on the machine running the tool has appropriate permissions to access the key and the secret. You may refer to this [document](https://docs.aws.amazon.com/secretsmanager/latest/userguide/auth-and-access.html)

7. Specify name and other details for the secret and other details and save the secret and note the name for it as it will be used in the next step.

#### Configuration file Inputs

Provide the details for the source, target, file-format, Secrets manager region and logging level in the configuration file **"configurations.ini"** in **conf** folder of tool source directory. Specify the details of the **Secrets Manager** secret for source and target, file format and logging details in the following format.

```text
[source]
SOURCE_DATABASE_TYPE = mssql
SOURCE_SECRET_ID = <source_database_secrets_id>

[target]
TARGET_DATABASE_TYPE = postgres
TARGET_SECRET_ID = <target_database_secrets_id>

[region]
SECRET_REGION = <secrets_manager_secret_region>

[file-format]
FILE_FORMAT = html

[logging]
DEBUG_LEVEL = INFO
```

The following are the allowed input values for **SOURCE_DATABASE_TYPE**, **TARGET_DATABASE_TYPE** and **FILE_FORMAT** respectively: 

```text
SQL Server = mssql
MySQL      = mysql
Oracle     = oracle
PostgreSQL = postgres

FILE_FORMAT = html 
FILE_FORMAT = xlsx
```

Also ensure that the AWS credentials have been setup on the machine where the tool is ran. You can follow this [document](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) for setting up access to AWS.

### Report Generation

To generate a output validation report, execute one of the following commands

```python
python src/main.py

python3 src/main.py
```

Note: In case of any error related to Microsoft ODBC Driver Manager, please install related driver from [link](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16) and re-execute the report generation command.

### Output Location

Tool will generate a report with name in format of "migration_summary_SOURCE_to_TARGET_TIMESTAMP.FILEFORMAT" to the **output** folder in the tool source directory.

```bash
Example: File - /Users/username/Downloads/multi-database-schema-validator-main/output/migration_summary_mssql_to_postgres_20230525_084217.html
```

### Logs Location

Detail level logging is written to the **logs** folder in the tool source directory.

```bash
Example: /Users/username/Downloads/multi-database-schema-validator-main/logs/run_log_20230525_084208.log
```

**Note:** These details can also be set using environment variables with the same names and those would override the details in the configuration 

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
