CREATE OR REPLACE TABLE customer (
    customer_id INT AUTOINCREMENT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    registration_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
 
 
INSERT INTO customer (first_name, last_name, email, phone, address, city, state, country, postal_code)
VALUES
    ('John', 'Doe', 'john.doe@example.com', '+1234567890', '123 Main St', 'Anytown', 'CA', 'USA', '12345'),
    ('Jane', 'Smith', 'jane.smith@example.com', '+1987654321', '456 Elm St', 'Smalltown', 'NY', 'USA', '54321'),
    ('Alice', 'Johnson', 'alice.johnson@example.com', '+1122334455', '789 Oak Ave', 'Bigcity', 'TX', 'USA', '67890'),
    ('Bob', 'Brown', 'bob.brown@example.com', '+1555666777', '321 Pine Rd', 'Villageton', 'FL', 'USA', '98765'),
    ('Emily', 'Davis', 'emily.davis@example.com', '+1444333222', '654 Cedar Ln', 'Ruraltown', 'GA', 'USA', '13579'),
    ('Michael', 'Wilson', 'michael.wilson@example.com', '+1888777666', '987 Birch Blvd', 'Metropolis', 'IL', 'USA', '24680'),
    ('Sarah', 'Martinez', 'sarah.martinez@example.com', '+1666555444', '234 Spruce Dr', 'Suburbia', 'WA', 'USA', '97531'),
    ('David', 'Garcia', 'david.garcia@example.com', '+1999888777', '876 Willow Ave', 'Outskirts', 'MI', 'USA', '80246'),
    ('Sophia', 'Lopez', 'sophia.lopez@example.com', '+1777666555', '567 Maple St', 'Distantville', 'OH', 'USA', '45678'),
    ('James', 'Gonzalez', 'james.gonzalez@example.com', '+1888999000', '432 Cherry Ct', 'Midtown', 'PA', 'USA', '54321');
 
 
create or replace stream customer_stream on table  customer;
 
 
select * from customer_stream;
 
 
INSERT INTO customer (first_name, last_name, email, phone, address, city, state, country, postal_code)
VALUES
    ('Jeri', 'jer', 'jer.doe@example.com', '+12367890', '123 Mn St', 'Anytwn', 'CAm', 'ind', '124645');
 
select * from customer_stream;
 
 
select * from customer;
 
delete from customer where customer_id = 11;
 
INSERT INTO customer (first_name, last_name, email, phone, address, city, state, country, postal_code)
VALUES ('Sophia', 'Lopez', 'sophia.lopez@example.com', '+1777666555', '567 Maple St', 'Distantville', 'OH', 'USA', '45678');
 
 
update customer
set first_name='test'
where customer_id = 1;
 
show streams;
alter stream customer_stream set comment='update done recently ';
 
desc stream customer_stream;
 
 
    CREATE OR REPLACE TABLE customer_details (
    customer_id INT AUTOINCREMENT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100));
 
 
    INSERT INTO customer_details (first_name, last_name, email)
VALUES('John', 'Doe', 'john.doe@example.com'),('Jane', 'Smith', 'jane.smith@example.com'),
('Alice', 'Johnson', 'alice.johnson@example.com');
 
 
create or replace stream customers_stream on table  customer_details 
append_only = TRUE;
 
select * from customer_details;
 
select * from customer_stream order by customer_id;

select * from customers_stream;

INSERT INTO customer_details (first_name, last_name, email)
VALUES('Bob', 'Brown', 'bob.brown@example.com');

update customer_details
set first_name = 'tes2' where customer_id = 3;
 
show streams;

CREATE OR REPLACE STREAM customer_update_stream ON TABLE customer;


SELECT c.*, s.* FROM customer c
JOIN customer_stream s ON c.customer_id = s.customer_id;



ALTER STREAM customer_stream SET RETENTION_TIME_IN_DAYS = 30;



SHOW STREAMS like '%customer%';

show streams;
SELECT *
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "table_name" ILIKE '%customer';

