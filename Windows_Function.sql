create database rec_test;

create schema rec_schema;

CREATE SEQUENCE SEQ_EMPLOYEES;

CREATE TABLE employees (
    id INTEGER DEFAULT SEQ_EMPLOYEES.NEXTVAL PRIMARY KEY,
    name VARCHAR(100),
    manager_id INTEGER
);

INSERT INTO employees (name, manager_id) VALUES
('Alice', NULL),
('Bob', 1),
('Charlie', 1),
('Dave', 2),
('Eve', 2),
('Frank', 3);



WITH RECURSIVE employee_hierarchy AS (
    SELECT
        id,
        name,
        manager_id,
        1 AS level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL
    SELECT
        e.id,
        e.name,
        e.manager_id,
        eh.level + 1 AS level
    FROM employees e
    INNER JOIN employee_hierarchy eh ON e.manager_id = eh.id
)select * from employee_hierarchy


SHOW SEQUENCES;


select * from employees



--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



CREATE TABLE product_groups (
	group_id Integer PRIMARY KEY,
	group_name VARCHAR (255) NOT NULL
);

CREATE TABLE products (
	product_id integer PRIMARY KEY,
	product_name VARCHAR (255) NOT NULL,
	price DECIMAL (11, 2),
	group_id INT NOT NULL,
	FOREIGN KEY (group_id) REFERENCES product_groups (group_id)
);


INSERT INTO product_groups
VALUES
	(1,'Smartphone'),
	(2,'Laptop'),
	(3,'Tablet');

    
INSERT INTO products (product_id,product_name,group_id,price)
VALUES
	(1,'Microsoft Lumia', 1, 200),
	(2,'HTC One', 1, 400),
	(3,'Nexus', 1, 500),
	(4,'iPhone', 1, 900),
	(5,'HP Elite', 2, 1200),
	(6,'Lenovo Thinkpad', 2, 700),
	(7,'Sony VAIO', 2, 700),
	(8,'Dell Vostro', 2, 800),
	(9,'iPad', 3, 700),
	(10,'Kindle Fire', 3, 150),
	(11,'Samsung Galaxy Tab', 3, 200);


SELECT
	AVG (price)
FROM
	products;

SELECT
	group_name,
	round(AVG (price))
FROM
	products
INNER JOIN product_groups USING (group_id)
GROUP BY group_name;



SELECT
	product_name,
	price,
	group_name,
	round(AVG (price) OVER (
	   PARTITION BY group_name
	))
FROM
	products
	INNER JOIN 
		product_groups USING (group_id);



SELECT
	product_name,
	group_name,
  price,
	RANK () OVER (
		PARTITION BY group_name
		ORDER BY price
	) as rn
FROM
	products
INNER JOIN product_groups USING (group_id);



SELECT
	product_name,
	group_name,
	price,
	DENSE_RANK () OVER (
		PARTITION BY group_name
		ORDER BY
			price
	)
FROM
	products
INNER JOIN product_groups USING (group_id);



select 	product_name,
	group_name,
	price,
-- LAST_VALUE (price) OVER (
	FIRST_VALUE (price) OVER (
		PARTITION BY group_name
		ORDER BY price desc
	) AS lowest_price_per_group 
FROM products
INNER JOIN product_groups USING (group_id);


SELECT
	product_name,
	group_name,
	price,
	LAG (price, 1) OVER (
		PARTITION BY group_name
		ORDER BY price
	) AS prev_price,
	price - LAG (price, 1) OVER (
		PARTITION BY group_name
		ORDER BY
			price
	) AS cur_prev_diff
FROM
	products
INNER JOIN product_groups USING (group_id);



SELECT
	product_name,
	group_name,
	price,
	LEAD (price, 1) OVER (
		PARTITION BY group_name
		ORDER BY
			price
	) AS next_price,
	LEAD (price, 1) OVER (
		PARTITION BY group_name
		ORDER BY
			price
	)- price AS cur_next_diff
FROM
	products
INNER JOIN product_groups USING (group_id);



SELECT
        product_name,
        group_name,
        price,
        ROW_NUMBER () OVER (
                PARTITION BY group_name
                ORDER BY price
        ) as rn
FROM
        products
INNER JOIN product_groups USING (group_id);



--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE TABLE sales_stats (
    name VARCHAR(100) NOT NULL,
    year SMALLINT NOT NULL,
    amount DECIMAL(10,2),
    PRIMARY KEY (name, year)
);


INSERT INTO 
    sales_stats(name, year, amount)
VALUES
    ('John Doe',2018,120000),
    ('Jane Doe',2018,110000),
    ('Jack Daniel',2018,150000),
    ('Yin Yang',2018,30000),
    ('Stephane Heady',2018,200000),
    ('John Doe',2019,150000),
    ('Jane Doe',2019,130000),
    ('Jack Daniel',2019,180000),
    ('Yin Yang',2019,25000),
    ('Stephane Heady',2019,270000);


SELECT 
    name,
    year, 
    amount,
    CUME_DIST() OVER (
        ORDER BY amount
    ) cum_dist
FROM 
    sales_stats
WHERE 
    year = 2018;


SELECT 
	name,
	amount,
	NTILE(3) OVER(
		ORDER BY amount
	)
FROM
	sales_stats
WHERE
	year = 2019;


SELECT 
    product_id,
    product_name,
    price,
    NTH_VALUE(product_name, 3) 
    OVER(
        ORDER BY price DESC
        RANGE BETWEEN 
            UNBOUNDED PRECEDING AND 
            UNBOUNDED FOLLOWING
    )
FROM 
    products
    order by price desc;


SELECT 

    name,
	amount,
    PERCENT_RANK() OVER (
        ORDER BY amount
    )
FROM 
    sales_stats
WHERE 
    year = 2019;


SELECT 
name,
amount,
PERCENT_RANK() OVER (
	PARTITION BY year
	ORDER BY amount
)
FROM 
sales_stats;




select * from STREAM_TEST.INFORMATION_SCHEMA.HYBRID_TABLES