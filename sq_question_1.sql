  create table dept (dept_id integer , name varchar(50),
                     Primary key(dept_id));
  
  insert into dept values (101,'IT');
   insert into dept values (102,'Finance');
   insert into dept values (103,'Sales');
   insert into dept values (104,'Research');
   insert into dept values (105,'Technical');

create table employee (  emp_id integer,
                        name varchar(100),
                        dept_id integer,
                        mgr_id integer,
                        salary integer, 
                        joining_date varchar(100),
                       termination_date varchar(100), 
                       primary key(emp_id));


 insert into employee values (1,'Harish',NULL,NULL,90000,'1990-12-17','2015-12-17');
 insert into employee values (2,'Hari',NULL,NULL,80000,'1991-02-17',NULL);
 insert into employee values (3,'Aneesh',101,1,18000,'1996-10-10',NULL);
 insert into employee values (4,'Manish',102,2,25000,'1997-11-12',NULL);
 insert into employee values (5,'Krishna',103,1,35000,'1998-12-17',NULL);
 insert into employee values  (6,'Mani',104,2,15500,'1999-02-16',NULL);
 insert into employee values (7,'Tamil',105,1,20000,'2000-11-16','2018-12-17');
 insert into employee values (8,'Mano',101,2,45000,'2001-10-13',NULL);
 insert into employee values (9,'Vishnu',102,1,35000,'2002-03-15','2015-11-17');
 insert into employee values (10,'Anish',103,2,55000,'2001-03-15',NULL);
 insert into employee values (11,'Siva',104,1,70000,'2003-03-15',NULL);
 insert into employee values (12,'Krish',105,2,15000,'2004-04-14',NULL);
 insert into employee values (13,'Swamy',101,1,14000,'2005-05-16',NULL);
 insert into employee values (14,'Aarav',102,3,13000,'2006-06-15','2015-12-17');
 insert into employee values (15,'Prasanth',103,4,12000,'2007-07-17','2015-12-17');
 insert into employee values (16,'Prakasam',104,5,11000,'2008-08-18',NULL);
 insert into employee values (17,'Pranith',105,6,10000,'2009-09-19',NULL);
 insert into employee values (18,'Harinarayanan',101,7,16000,'2010-10-20',NULL);
 insert into employee values (19,'Tamilselvan',102,8,17000,'2011-11-21',NULL);
 insert into employee values (20,'Arun',103,9,18000,'2012-12-22',NULL);
 insert into employee values (21,'Selvam',101,10,19000,'2013-01-23','2019-12-17');
 insert into employee values (22,'Somu',104,11,20000,'2014-02-24',NULL);
 insert into employee values (23,'Aravind',105,12,21000,'2015-03-25','2018-12-17');
 insert into employee values (24,'Madhan',101,13,22000,'2016-04-26',NULL);
 insert into employee values (25,'Shahid',101,3,23000,'2010-03-20','2015-12-17');
 insert into employee values (26,'Deepak',102,4,24000,'2017-05-27',NULL);
 insert into employee values (27,'Deepthi',103,5,25000,'2008-08-28',NULL);
 insert into employee values (28,'Deepika',104,6,26000,'2009-09-29',NULL);
 insert into employee values (29,'Anu',105,7,27000,'2010-10-30','2015-12-15');
 insert into employee values (30,'Priya',102,8,28000,'2013-03-23',NULL);
 insert into employee values (31,'karan',101,9,29000,'2001-01-01',NULL);
 insert into employee values (32,'Aarush',103,10,30000,'1999-05-23',NULL);
 insert into employee values (33,'Aayush',104,11,31000,'2000-05-20',NULL);
 insert into employee values (34,'Akarsh',105,12,32000,'2017-05-20',NULL);
 insert into employee values (35,'Anirudh',101,13,33000,'2016-04-24',NULL);
 insert into employee values (36,'Anitha',102,3,34000,'2003-01-30',NULL);
 insert into employee values (37,'Sharan',103,4,35000,'2004-05-27',NULL);
 insert into employee values (38,'Darshit',101,5,36000,'2012-05-28',NULL);
 insert into employee values (39,'Devansh',104,6,37000,'2013-02-28',NULL);
 insert into employee values (40,'Deepa',105,7,38000,'2010-04-10',NULL);
 insert into employee values (41,'Deepansh',102,8,39000,'2000-07-17',NULL);
 insert into employee values (42,'Hiran',101,9,40000,'2010-06-26','2018-12-17');
 insert into employee values (43,'Indra',103,10,41000,'2001-02-27',NULL);
 insert into employee values (44,'Jayesh',104,11,42000,'2006-11-12',NULL);
 insert into employee values (45,'Ranbir',105,12,43000,'2012-12-12',NULL);
 insert into employee values (46,'Kapoor',101,13,44000,'2000-05-20',NULL);
 insert into employee values (47,'Khan',102,3,45000,'2004-06-21',NULL);
 insert into employee values (48,'Salman',103,4,46000,'2005-03-29',NULL);
 insert into employee values (49,'Shreyas',104,5,47000,'2010-07-20','2015-02-17');
 insert into employee values (50,'Sushil',105,6,48000,'2011-08-20',NULL);
 insert into employee values (51,'Bhavya',101,7,49000,'2014-08-10',NULL);
 insert into employee values (52,'David',102,8,50000,'2016-04-11',NULL);
 insert into employee values (53,'Shibani',103,9,51000,'2010-12-11','2016-12-17');
 insert into employee values (54,'Manikavel',104,10,52000,'2014-02-05','2018-12-17');
 insert into employee values (55,'Arshiya',105,11,53000,'2015-03-12',NULL);
 insert into employee values (56,'Divya',101,12,54000,'2016-04-15',NULL);
 insert into employee values (57,'Praveen',102,13,55000,'2017-02-05',NULL);
 insert into employee values (58,'Shanmugam',101,3,56000,'2014-02-05',NULL);
 insert into employee values (59,'Priyan',103,4,57000,'2013-04-05',NULL);
 insert into employee values (60,'Manisha',104,5,58000,'2018-05-08',NULL);
 insert into employee values (61,'Anisha',105,6,59000,'2012-10-05',NULL);
 insert into employee values (62,'Priyanka',101,7,60000,'2015-11-05',NULL);
 insert into employee values (63,'Prathiban',104,8,61000,'2016-07-07',NULL);
 insert into employee values (64,'Harshitha',103,9,62000,'2014-08-08',NULL);
 insert into employee values (65,'Ashwitha',101,10,63000,'2015-02-05','2019-02-17');
 insert into employee values (66,'Ashok',102,11,64000,'2000-02-05',NULL);
 insert into employee values (67,'Vidhya',103,12,65000,'2003-05-05',NULL);
 insert into employee values (68,'Jaya',104,13,66000,'2016-03-05',NULL);
 insert into employee values (69,'Raj',105,3,67000,'2001-02-09',NULL);
 insert into employee values (70,'Gowtham',101,4,68000,'2015-02-06',NULL);
 insert into employee values (71,'Gopi',102,5,69000,'2002-10-15',NULL);
 insert into employee values (72,'Gopinath',103,6,70000,'2003-03-05',NULL);
 insert into employee values (73,'Rubin',104,7,15000,'2011-02-05',NULL);
 insert into employee values (74,'Rakesh',105,8,25000,'1999-02-03',NULL);
 insert into employee values (75,'Pradeep1',101,9,35000,'1998-03-09',NULL);


1. Write a SQL to find the below details
        - Manager Name A, Manager Name B, Manager Name A Emp Count,Manager Name B Emp Count
        - where number of employees joined under Manager A is more than number of Employees joined under B after the year 2020


        WITH ManagerCounts AS (
            SELECT
                mgr_id,
                COUNT(*) AS emp_count
            FROM
                employee
            WHERE
                joining_date >= '2010-01-01'
            GROUP BY
                mgr_id
        ),
        ManagerNames AS (
            SELECT
                e1.mgr_id AS manager_id,
                e2.name AS manager_name
            FROM
                employee e1
            JOIN
                employee e2 ON e1.mgr_id = e2.emp_id
            GROUP BY
                e1.mgr_id, e2.name
        ),
        RankedManagers AS (
            SELECT
                m1.manager_name AS manager_a,
                m2.manager_name AS manager_b,
                COALESCE(mc1.emp_count, 0) AS manager_a_emp_count,
                COALESCE(mc2.emp_count, 0) AS manager_b_emp_count
            FROM
                ManagerNames m1
            JOIN
                ManagerCounts mc1 ON m1.manager_id = mc1.mgr_id
            JOIN
                ManagerNames m2 ON m1.manager_id != m2.manager_id
            LEFT JOIN
                ManagerCounts mc2 ON m2.manager_id = mc2.mgr_id
            WHERE
                mc1.emp_count > COALESCE(mc2.emp_count, 0)
        )
        SELECT
            manager_a,
            manager_b,
            manager_a_emp_count,
            manager_b_emp_count
        FROM
            RankedManagers;


2. Write a SQL Function get_Manager_Emp_List(mgr_id)
    emp_id, Emp_name, level
        Eg: A reports to B and B reports to C then for input C output should be as follows
            get_Manager_Emp_List(C)
            B, 1
            A, 2
            get_Manager_Emp_List(B)
            A,1

CREATE OR REPLACE PROCEDURE get_Manager_Emp_List(mgr_id INTEGER)
RETURNS TABLE (emp_id INTEGER, emp_name VARCHAR, level INTEGER)
LANGUAGE SQL
AS
$$
    DECLARE
        resultset RESULTSET DEFAULT (
            WITH RECURSIVE EmployeeHierarchy AS (
                SELECT 
                    emp_id,
                    name AS emp_name,
                    1 AS level
                FROM 
                    employee
                WHERE 
                    mgr_id = mgr_id

                UNION ALL
                SELECT 
                    e.emp_id,
                    e.name AS emp_name,
                    eh.level + 1 AS level
                FROM 
                    employee e
                    INNER JOIN EmployeeHierarchy eh ON e.mgr_id = eh.emp_id
            )
            SELECT 
                emp_id, emp_name, level
            FROM 
                EmployeeHierarchy
        );
    BEGIN
        RETURN TABLE(resultset);
    END;
$$;


CALL get_Manager_Emp_List(1);





3. Write a SQL Function get_Emp_Peers(Emp_id)
    Find out the list of peers for the given emp in the below format
        emp_id, emp_name, percentile rank of salary when compared to the peer


CREATE OR REPLACE PROCEDURE get_Emp_Peers(emp_id INTEGER)
RETURNS TABLE (peer_emp_id INTEGER, peer_emp_name VARCHAR, percentile_rank FLOAT)
LANGUAGE SQL
AS
$$
    DECLARE
        resultset RESULTSET DEFAULT (
            SELECT
                e2.emp_id AS peer_emp_id,
                e2.name AS peer_emp_name,
                PERCENT_RANK() OVER (PARTITION BY e1.dept_id ORDER BY e2.salary) AS percentile_rank
            FROM
                employee e1
            JOIN
                employee e2
            ON
                e1.dept_id = e2.dept_id
            WHERE
                e1.emp_id = :emp_id
                AND e2.emp_id != :emp_id
        );
    BEGIN
        RETURN TABLE(resultset);
    END;
$$;


CALL get_Emp_Peers(11);