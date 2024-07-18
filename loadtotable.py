# Python code to create table and load data to table

import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

def main(session: Session):
    stage_file_path = '@"LOADTABLE"."LOADTABLE"."LOADTABLE"/outputscopy.csv'

    filename = os.path.basename(stage_file_path).split('.')[0]
    table_name = filename
    print("TableName: "+table_name)

    try:
        session.sql(f"""
            CREATE OR REPLACE TABLE inferred_schema AS
            SELECT COLUMN_NAME, EXPRESSION 
            FROM TABLE(INFER_SCHEMA(
                LOCATION => '{stage_file_path}',
                FILE_FORMAT => 'my_csv_format'
            ))
        """).collect()
        
        create_table_stmt = session.sql("""
            SELECT 'CREATE OR REPLACE TABLE {table_name} (' || 
                   LISTAGG(COLUMN_NAME || ' ' || 
                       CASE 
                           WHEN EXPRESSION LIKE '%::NUMBER%' THEN 'NUMBER'
                           WHEN EXPRESSION LIKE '%::TEXT%' THEN 'TEXT'
                           WHEN EXPRESSION LIKE '%::DATE%' THEN 'DATE'
                           ELSE 'TEXT'
                       END, ', ') || 
                   ')' AS create_table_statement
            FROM inferred_schema
        """).collect()[0][0]
        
        create_table_stmt = create_table_stmt.replace('{table_name}', table_name)
        
        session.sql(create_table_stmt).collect()
        
        session.sql(f'TRUNCATE TABLE IF EXISTS {table_name}').collect()
        
        session.sql(f"""
            COPY INTO {table_name}
            FROM {stage_file_path}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'SKIP_FILE'
        """).collect()
        
        print("Data loaded successfully."+table_name)
        
        success_df = session.create_dataframe([("Table : "+table_name +" created and Data loaded successfully",)]).to_df("message")
        return success_df
    except Exception as e:
        print(f"An error occurred: {e}")
        error_df = session.create_dataframe([(f"An error occurred: {e}",)]).to_df("message")
        return error_df




####################################################################################################################################################################################
    

# Python code with warehouse switching 


import os
from snowflake.snowpark import Session

def main(session: Session):
    # Stage file path
    stage_file_path = '@"LOADTABLE"."LOADTABLE"."LOADTABLE"/outputscopy.csv'

    # Extract the filename (without extension) to use as the table name
    filename = os.path.basename(stage_file_path).split('.')[0]
    table_name = filename
    print("TableName: " + table_name)

    try:
        # Check file size of the staged file in Snowflake
        file_size_query = f"LIST {stage_file_path}"
        file_size_result = session.sql(file_size_query).collect()
        
        if file_size_result:
            # Assuming file size info is in the first row and second column
            file_size_bytes = int(file_size_result[0][1])
            print(f"File size: {file_size_bytes} bytes")
            
            # Determine warehouse based on file size
            if file_size_bytes > 100 * 1024:  # Check if file size is more than 100 KB
                warehouse_to_use = 'PYTHON_API_WH'
            else:
                warehouse_to_use = 'COMPUTE_WH'
        else:
            raise Exception("Failed to retrieve file size information.")
        
        # Switch warehouse based on the determined value
        session.sql(f'USE WAREHOUSE {warehouse_to_use}').collect()
        print(f"Using warehouse: {warehouse_to_use}")

        # Infer schema from the CSV file
        session.sql(f"""
            CREATE OR REPLACE TABLE inferred_schema AS
            SELECT COLUMN_NAME, EXPRESSION 
            FROM TABLE(INFER_SCHEMA(
                LOCATION => '{stage_file_path}',
                FILE_FORMAT => 'my_csv_format'
            ))
        """).collect()
        
        # Construct the SQL statement to create the table with inferred types
        create_table_stmt = session.sql("""
            SELECT 'CREATE OR REPLACE TABLE {table_name} (' || 
                   LISTAGG(COLUMN_NAME || ' ' || 
                       CASE 
                           WHEN EXPRESSION LIKE '%::NUMBER%' THEN 'NUMBER'
                           WHEN EXPRESSION LIKE '%::TEXT%' THEN 'TEXT'
                           WHEN EXPRESSION LIKE '%::DATE%' THEN 'DATE'
                           ELSE 'TEXT'
                       END, ', ') || 
                   ')' AS create_table_statement
            FROM inferred_schema
        """).collect()[0][0]
        
        # Replace {table_name} placeholder with actual table name
        create_table_stmt = create_table_stmt.replace('{table_name}', table_name)
        
        # Execute the create table statement
        session.sql(create_table_stmt).collect()
        
        # Remove existing data in the table (optional)
        session.sql(f'TRUNCATE TABLE IF EXISTS {table_name}').collect()
        
        # Copy data from stage file to the table, skipping the header row
        session.sql(f"""
            COPY INTO {table_name}
            FROM {stage_file_path}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            ON_ERROR = 'SKIP_FILE'
        """).collect()
        
        print(f"Data loaded successfully. Table name: {table_name}")
        
        # Return a DataFrame indicating success
        success_df = session.create_dataframe([("Table created and data loaded successfully. Table name: "+table_name,)]).to_df("message")
        return success_df
    except Exception as e:
        print(f"An error occurred: {e}")
        # Return a DataFrame indicating failure
        error_df = session.create_dataframe([(f"An error occurred: {e}",)]).to_df("message")
        return error_df

# Call the main function with the provided session
# Snowflake will automatically provide the session when this script is executed in a Snowflake worksheet or stored procedure context
