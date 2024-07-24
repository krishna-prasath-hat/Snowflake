cross db referencing 

CREATE OR REPLACE DATABASE sales_db;
CREATE OR REPLACE DATABASE marketing_db;
USE DATABASE sales_db;
CREATE OR REPLACE TABLE sales (
    sale_id INT,
    amount DECIMAL(10, 2),
    customer_id INT
);

INSERT INTO sales VALUES 
(1, 100.00, 1),
(2, 150.00, 2);

USE DATABASE marketing_db;

CREATE OR REPLACE TABLE customers (
    customer_id INT,
    name STRING,
    email STRING
);

INSERT INTO customers VALUES 
(1, 'Alice', 'alice@example.com'),
(2, 'Bob', 'bob@example.com');



use database sales_db;
SELECT 
    s.sale_id,
    s.amount,
    c.name,
    c.email
FROM sales_db.public.sales s
JOIN marketing_db.public.customers c
ON s.customer_id = c.customer_id;


select * from marketing_db.public.customers ;
select* from sales_db.public.sales;



SELECT 
    s.sale_id,
    s.amount,
    c.name,
    c.email
FROM sales_db.public.sales s
JOIN marketing_db.public.customers c
ON s.customer_id = c.customer_id;


use database marketing_db;

select * from customers


