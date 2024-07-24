create database unstructured;
create schema unstructured;

CREATE OR REPLACE STAGE my_external_stage
  URL='s3://snowflakebucket16789/Get_Started_With_Smallpdf.pdf'
  CREDENTIALS=(AWS_KEY_ID='' AWS_SECRET_KEY='');

  

CREATE OR REPLACE TABLE pdf_metadata (
  id INTEGER AUTOINCREMENT,
  filename STRING,
  s3_url STRING,
  upload_date TIMESTAMP
);


INSERT INTO pdf_metadata (filename, s3_url, upload_date)
VALUES
('file1.pdf', 's3://snowflakebucket16789/Get_Started_With_Smallpdf.pdf', CURRENT_TIMESTAMP);

SELECT * FROM pdf_metadata;

create table PDF_TEXT_DATA (filename STRING , text STRING, upload_date STRING);

select * from PDF_TEXT_DATA;