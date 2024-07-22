create or replace TABLE EXCEPTIONHANDLER.EXCEPTIONHANDLER.ERROR_LOG_TABLE (
	FUNCTION_NAME VARCHAR(16777216),
	TIMESTAMP TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	EXCEPTION_DESCRIPTION VARCHAR(16777216)
);

CREATE OR REPLACE PROCEDURE log_error(function_name STRING, exception_description STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO error_log_table (function_name, exception_description)
    VALUES (:function_name, :exception_description);
    RETURN 'Error logged successfully';
END;
$$;


CALL log_error('sample_procedure_with_error_handling', 'error message');


CREATE OR REPLACE PROCEDURE sample_procedure_with_error_handling()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var result = snowflake.execute({
        sqlText: `SELECT 1 / 0;`  
    });
    return 'Operation succeeded';
} catch (err) {
    var logResult = snowflake.execute({
        sqlText: `CALL log_error(?, ?);`,
        binds: ['sample_procedure_with_error_handling', err.message]
    });
    return 'Operation failed: ' + err.message;
}
$$;


CALL sample_procedure_with_error_handling();

select * from ERROR_LOG_TABLE
