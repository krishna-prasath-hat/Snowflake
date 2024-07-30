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

