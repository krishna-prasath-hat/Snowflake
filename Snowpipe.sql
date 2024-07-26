CREATE OR REPLACE STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 's3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::767398098944:role/snowflake_pipe'
    STORAGE_ALLOWED_LOCATIONS = ('s3://snowflakebucket16789/');


desc integration s3_int;

create or replace database snowpipe;

create or replace stage snowstage
url = 's3://snowflakebucket16789/'
storage_integration = s3_int;

show stages;

CREATE OR REPLACE TABLE snowpipe.public.snowtable(jsontext variant);

select * from snowtable;

CREATE OR REPLACE PIPE snowpipe.public.snowpipe auto_ingest=true as
 copy into snowpipe.public.snowtable
 from @snowpipe.public.snowstage
 file_format=(type = 'JSON');
 
show pipes;

select SYSTEM$PIPE_STATUS('snowpipe.public.snowpipe');   
select * from snowpipe.public.snowtable;

select * from table(information_schema.copy_history(table_name=>'snowpipe.public.snowtable',start_time=>dateadd(hours,-1,current_timestamp())));



