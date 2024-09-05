import snowflake.connector
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import fitz  # PyMuPDF

# Snowflake connection parameters
snowflake_account = 'oytvmyv-as20412'
snowflake_user = 'AKSHAY'
snowflake_password = 'Srini@1406'
snowflake_database = 'unstructured'
snowflake_schema = 'unstructured'
snowflake_table = 'pdf_text_data'

# AWS S3 parameters
aws_access_key_id = ''
aws_secret_access_key = ''

s3_bucket_name = 'snowflakebucket16789'
file_key = 'Get_Started_With_Smallpdf.pdf'

# Initialize Snowflake connection
conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    database=snowflake_database,
    schema=snowflake_schema
)

# Initialize S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

try:
    # Download file from S3 as binary
    response = s3.get_object(Bucket=s3_bucket_name, Key=file_key)
    file_content = response['Body'].read()  # Read as binary data

    # Save the file locally
    with open('/tmp/temp_pdf.pdf', 'wb') as f:
        f.write(file_content)

    # Extract text from PDF
    pdf_document = fitz.open('/tmp/temp_pdf.pdf')
    text_content = ""
    for page in pdf_document:
        text_content += page.get_text()
    pdf_document.close()

    # Prepare Snowflake INSERT statement
    insert_stmt = f"INSERT INTO {snowflake_table} (filename, text, upload_date) VALUES (%s, %s, %s)"

    # Execute INSERT statement
    with conn.cursor() as cursor:
        cursor.execute(insert_stmt, (file_key, text_content, datetime.utcnow()))

    conn.commit()
    print("Data inserted successfully into Snowflake.")

except ClientError as e:
    print(f"Error: {e.response['Error']['Message']}")

finally:
    conn.close()

