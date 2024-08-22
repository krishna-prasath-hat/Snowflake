# Import Libraries 
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col 

def create_session_object():

    connection_parameters = {
        "account": "KLUWWWQ-XN81642",
        "user": "Jerish",
        "password": "Mahendrasingh@7",
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "STREAM_TEST",
        "schema": "STREAM_TEST"
    }

    session = Session.builder.configs(connection_parameters).create()
    return session

def create_dataframe(session):
  
    df_table = session.table("CUSTOMER")
    df_table.count()
    df_table.show()
    df_results = df_table.collect()
    df_filtered = df_table.filter(col("POSTAl_CODE") > 30000).sort(col("POSTAl_CODE").desc()).limit(10)

    df_filtered.show()
  
    df_filtered.collect()
  
    df_filtered_persisted = df_table.collect()
    # print(df_filtered_persisted)
# ------------------------------------------------------------------------------------------------------------------

# FUNCTION CALLS

# call session object
session = create_session_object()

# call create dataframe 
_ = create_dataframe(session) 

# end your session
session.close()