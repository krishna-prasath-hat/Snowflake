WITH EmployeeCount AS (
    SELECT 
        manager_id, 
        COUNT(*) AS emp_count
    FROM 
        employee
    WHERE 
        join_date > '2020-01-01'
    GROUP BY 
        manager_id
), ManagerComparison AS (
    SELECT 
        e1.manager_id AS manager_a_id, 
        e2.manager_id AS manager_b_id,
        e1.emp_count AS manager_a_emp_count, 
        e2.emp_count AS manager_b_emp_count,
        m1.manager_name AS manager_name_a,
        m2.manager_name AS manager_name_b
    FROM 
        EmployeeCount e1
    JOIN 
        EmployeeCount e2 ON e1.emp_count > e2.emp_count
    JOIN 
        managers m1 ON e1.manager_id = m1.manager_id
    JOIN 
        managers m2 ON e2.manager_id = m2.manager_id
)
SELECT 
    manager_name_a AS Manager_Name_A,
    manager_name_b AS Manager_Name_B,
    manager_a_emp_count AS Manager_Name_A_Emp_Count,
    manager_b_emp_count AS Manager_Name_B_Emp_Count
FROM 
    ManagerComparison;

