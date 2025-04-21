CREATE OR REPLACE STORAGE INTEGRATION s3_int_csv
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 's3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::767398098944:role/snow_pipe'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflakebucket16789/');

CREATE OR REPLACE STAGE snowstage_csv
    URL = 's3://snowflakebucket16789/'
    STORAGE_INTEGRATION = s3_int_csv;


    CREATE OR REPLACE TABLE snowpipe.public.patents (
    patent_id INT,
    patent_type STRING,
    patent_date DATE,
    patent_title STRING,
    wipo_kind STRING,
    num_claims INT,
    withdrawn BOOLEAN,
    filename STRING
);


CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ',';


CREATE OR REPLACE PIPE snowpipe.public.snowpipe AUTO_INGEST=TRUE AS
COPY INTO snowpipe.public.patents
FROM @snowpipe.public.snowstage_csv
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');


SELECT SYSTEM$PIPE_STATUS('snowpipe.public.snowpipe');


SELECT * FROM snowpipe.public.patents;

SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => 'snowpipe.public.patents', START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())));





------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- New loading 



-- drop database snow_pipe;
create database snow_pipe;

create schema snow_pipe;

CREATE OR REPLACE STAGE snowflake_stage
URL = 's3://snowflakebucket16789/'
CREDENTIALS = (
  AWS_KEY_ID = ''
  AWS_SECRET_KEY = ''
);


CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('', 'NULL');


CREATE OR REPLACE TABLE fund_data (
  FUND STRING,
  DATE DATE,
  COMPANY_NAME STRING,
  TOTAL_SHARES NUMBER,
  CHANGES_IN_SHARE NUMBER,
  MARKET_CAP FLOAT,
  PRICE FLOAT,
  PRICE_50D_MOVING_AVERAGE FLOAT,
  PRICE_200D_MOVING_AVERAGE FLOAT
);



CREATE OR REPLACE PIPE fund_data_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO fund_data
  FROM @SNOWFLAKE_STAGE
  FILE_FORMAT = (FORMAT_NAME = my_csv_format);


show pipes;

LIST @SNOWFLAKE_STAGE;

-- -- manually triggering

-- COPY INTO fund_data
-- FROM @SNOWFLAKE_STAGE
-- FILE_FORMAT = (FORMAT_NAME = my_csv_format);


select SYSTEM$PIPE_STATUS('snow_pipe.snow_pipe.fund_data_pipe');



select * from fund_data;

select count(*) from fund_data;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'FUND_DATA',
  START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())));

