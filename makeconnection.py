import sys
from snowflake.snowpark import session
from generic_code import code_library

connection_parameters = {
    "account": "UCCLSFJ-KZ45288",
    "user": "RAGHU2110",
    "password": "Raghuveer2110#",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "DEMO_DB",
    "schema": "test"
}

session = code_library.snowconnection(connection_parameters)

def snowconnection(config: dict) -> session:
    session = session.builder.configs(config).create()

    # Create the audit table if it doesn't exist
    session.sql("""
        CREATE TABLE IF NOT EXISTS session_audit (
            session_id STRING,
            user_name STRING,
            warehouse STRING,
            role STRING,
            login_time TIMESTAMP
        )
    """).collect()


    # Insert session info
    session.sql("""
        INSERT INTO session_audit
        SELECT 
            CURRENT_SESSION(), 
            CURRENT_USER(), 
            CURRENT_WAREHOUSE(), 
            CURRENT_ROLE(), 
            CURRENT_TIMESTAMP()
    """).collect()

    return session

session = snowconnection(connection_parameters)