SELECT * 
FROM TABLE(INFER_SCHEMA(
  LOCATION => '@loadtable/outputs.csv',
  FILE_FORMAT => 'my_file_format'
));

 select $1, from @loadtable/outputs.csv

 SELECT t.$1, t.$2, t.$3, t.$4, t.$5,t.$6, t.$7, t.$8
FROM @loadtable/outputs.csv t


CREATE OR REPLACE FILE FORMAT my_csv_format 
TYPE = 'CSV' 
PARSE_HEADER = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

SELECT * 
FROM TABLE(INFER_SCHEMA(
  LOCATION => '@loadtable/outputs.csv',
  FILE_FORMAT => 'my_csv_format'
));

  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(COLUMN_NAME, COLUMN_TYPE)) 
  FROM TABLE(INFER_SCHEMA(
    LOCATION => '@my_stage',
    FILE_FORMAT => 'my_file_format',
    COLUMN_NAME => TRUE
  ))
);


CREATE TABLE my_table2 USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(COLUMN_NAME, COLUMN_TYPE)) 
  FROM TABLE(INFER_SCHEMA(
    LOCATION => '@my_stage',
    FILE_FORMAT => 'my_file_format',
    COLUMN_MAPPING => 'COLUMN_NAME'
  ))
);

  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) 
  FROM TABLE(INFER_SCHEMA(
    LOCATION => '@loadtable/outputs.csv',
    FILE_FORMAT => 'my_file_format'
  )
);



-- Query to retrieve file size of a specific file staged in Snowflake
SELECT METADATA$FILENAME AS file_name,
FROM '@"LOADTABLE"."LOADTABLE"."LOADTABLE"/outputscopy.csv';


SELECT METADATA$FILENAME,METADATA$FILE_SIZE AS file_size_bytes FROM '@"LOADTABLE"/outputscopy.csv' t;

SELECT 
  COLUMN_NAME
FROM TABLE(INFER_SCHEMA(
  LOCATION => '@loadtable/outputs.csv',
  FILE_FORMAT => 'my_file_format'
));


SELECT *
FROM INFORMATION_SCHEMA.STAGES
WHERE STAGE_NAME = 'LOADTABLE';


-- Query to sum up the size of files in a specific stage
SELECT SUM(FILE_SIZE) AS total_size_bytes
FROM INFORMATION_SCHEMA.FILES
WHERE STAGE_NAME = 'LOADTABLE';


SHOW tables


-- Example to sum up the size of files in the 'LOADTABLE' stage
SELECT SUM(FILE_SIZE) AS total_size_bytes
FROM INFORMATION_SCHEMA.FILES
WHERE STAGE_NAME = 'LOADTABLE';


SHOW FILES IN @LOADTABLE;

LIST @loadtable/outputs.csv;

