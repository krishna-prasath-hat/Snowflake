EXECUTE IMMEDIATE  :

-- Create Database and Schema
CREATE DATABASE EXECUTION;
USE DATABASE EXECUTION;

CREATE SCHEMA EXECUTION;
USE SCHEMA EXECUTION;

-- Create Table
CREATE TABLE STUDENTS (
    NAME STRING,
    AGE INT,
    PHONE_NO STRING,
    EMAIL STRING
);

-- Insert 25 Values
INSERT INTO STUDENTS (NAME, AGE, PHONE_NO, EMAIL) VALUES
('John Doe', 20, '1234567890', 'john.doe@example.com'),
('Jane Smith', 21, '2345678901', 'jane.smith@example.com'),
('Alice Johnson', 22, '3456789012', 'alice.johnson@example.com'),
('Bob Brown', 23, '4567890123', 'bob.brown@example.com'),
('Charlie Davis', 24, '5678901234', 'charlie.davis@example.com'),
('David Wilson', 20, '6789012345', 'david.wilson@example.com'),
('Eva Martinez', 21, '7890123456', 'eva.martinez@example.com'),
('Frank Moore', 22, '8901234567', 'frank.moore@example.com'),
('Grace Taylor', 23, '9012345678', 'grace.taylor@example.com'),
('Hank Anderson', 24, '0123456789', 'hank.anderson@example.com'),
('Ivy Thomas', 20, '1123456789', 'ivy.thomas@example.com'),
('Jack White', 21, '1223456789', 'jack.white@example.com'),
('Karen Harris', 22, '1323456789', 'karen.harris@example.com'),
('Larry Martin', 23, '1423456789', 'larry.martin@example.com'),
('Mia Clark', 24, '1523456789', 'mia.clark@example.com'),
('Nina Lewis', 20, '1623456789', 'nina.lewis@example.com'),
('Oscar Lee', 21, '1723456789', 'oscar.lee@example.com'),
('Pamela Walker', 22, '1823456789', 'pamela.walker@example.com'),
('Quinn Hall', 23, '1923456789', 'quinn.hall@example.com'),
('Rachel Young', 24, '2023456789', 'rachel.young@example.com'),
('Sam King', 20, '2123456789', 'sam.king@example.com'),
('Tina Scott', 21, '2223456789', 'tina.scott@example.com'),
('Uma Green', 22, '2323456789', 'uma.green@example.com'),
('Vince Adams', 23, '2423456789', 'vince.adams@example.com'),
('Wendy Baker', 24, '2523456789', 'wendy.baker@example.com');

-- Execute the SQL statement
EXECUTE IMMEDIATE $$SELECT * FROM STUDENTS WHERE AGE > 20$$;

set statement = 'SELECT * FROM STUDENTS WHERE AGE > 20';

EXECUTE IMMEDIATE $statement;


EXECUTE AS CALLER;
SELECT * FROM STUDENTS WHERE AGE > 20;
