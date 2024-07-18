CREATE OR REPLACE FILE FORMAT my_csv_format 
TYPE = 'CSV' 
PARSE_HEADER = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"';


-- SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) 
--   FROM TABLE(INFER_SCHEMA(
--     LOCATION => '@loadtable/outputs.csv',
--     FILE_FORMAT => 'my_csv_format'
--   )
-- );

-- SELECT 
--   COLUMN_NAME
-- FROM TABLE(INFER_SCHEMA(
--   LOCATION => '@loadtable/outputs.csv',
--   FILE_FORMAT => 'my_csv_format'
-- ));


CREATE OR REPLACE TABLE :inferred_schema AS
SELECT COLUMN_NAME, EXPRESSION 
FROM TABLE(INFER_SCHEMA(
  LOCATION => '@loadtable/outputs.csv',
  FILE_FORMAT => 'my_csv_format'
));


SET create_table_stmt = (
  SELECT 'CREATE OR REPLACE TABLE patents (' || 
         LISTAGG(COLUMN_NAME || ' ' || 
           CASE 
             WHEN EXPRESSION LIKE '%::NUMBER%' THEN 'NUMBER'
             WHEN EXPRESSION LIKE '%::TEXT%' THEN 'TEXT'
             WHEN EXPRESSION LIKE '%::DATE%' THEN 'DATE'
             ELSE 'TEXT'
           END, ', ') || 
         ')' AS create_table_statement
  FROM inferred_schema
);


SELECT $create_table_stmt;


EXECUTE IMMEDIATE $create_table_stmt;

CREATE OR REPLACE FILE FORMAT my_csv_format 
TYPE = 'CSV' 
FIELD_OPTIONALLY_ENCLOSED_BY = '"';


COPY INTO patents
FROM @loadtable/outputs.csv
FILE_FORMAT = 'my_csv_format'
ON_ERROR = 'CONTINUE';



-- select * from patents



------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


Stored procedure to load data to table from stage file



CREATE OR REPLACE PROCEDURE sp_loadToDB(stagename STRING, filename STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  create_table_stmt STRING;
  table_name STRING;
BEGIN
  table_name := SPLIT_PART(filename, '.', 1);

  EXECUTE IMMEDIATE '
    CREATE OR REPLACE FILE FORMAT my_csv_format2 
    TYPE = ''CSV'' 
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = ''"''
  ';
  
  EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS inferred_schema';
  
  EXECUTE IMMEDIATE '
    CREATE OR REPLACE TABLE inferred_schema AS
    SELECT COLUMN_NAME, EXPRESSION 
    FROM TABLE(INFER_SCHEMA(
      LOCATION => ''' || stagename || '/' || filename || ''',
      FILE_FORMAT => ''my_csv_format2''
    ))
  ';
  
  -- Create a temporary table to store the dynamic SQL statement
  EXECUTE IMMEDIATE 'CREATE TEMP TABLE temp_table_stmt(create_table_statement STRING)';

  -- Insert the create table statement into the temporary table
  EXECUTE IMMEDIATE '
    INSERT INTO temp_table_stmt
    SELECT ''CREATE OR REPLACE TABLE '' || ''' || table_name || ''' || '' ('' || 
           LISTAGG(COLUMN_NAME || '' '' || 
             CASE 
               WHEN EXPRESSION LIKE ''%::NUMBER%'' THEN ''NUMBER''
               WHEN EXPRESSION LIKE ''%::TEXT%'' THEN ''TEXT''
               WHEN EXPRESSION LIKE ''%::DATE%'' THEN ''DATE''
               ELSE ''TEXT''
             END, '', '') || 
           '')'' AS create_table_statement
    FROM inferred_schema
  ';

  -- Retrieve the create table statement from the temporary table
  SELECT create_table_statement INTO create_table_stmt FROM temp_table_stmt;

  -- Drop the temporary table
  EXECUTE IMMEDIATE 'DROP TABLE temp_table_stmt';

  -- Execute the create table statement
  EXECUTE IMMEDIATE create_table_stmt;

    EXECUTE IMMEDIATE '
    CREATE OR REPLACE FILE FORMAT my_csv_format2 
    TYPE = ''CSV'' 
    FIELD_OPTIONALLY_ENCLOSED_BY = ''"''
  ';
  
  
  -- Uncomment the following lines to actually load data into the table
  EXECUTE IMMEDIATE '
    COPY INTO ' || table_name || '
    FROM ''' || stagename || '/' || filename || '''
    FILE_FORMAT = ''my_csv_format2''
    ON_ERROR = ''CONTINUE''
  ';

  RETURN 'Table ' || table_name || ' created and loaded data successfully. ';
  EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS inferred_schema';

END;
$$;



CALL sp_loadToDB('@LOADTABLE', 'outputs.csv');
