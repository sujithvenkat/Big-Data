import os
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Hive_Workouts").master("local[*]").enableHiveSupport().getOrCreate()

#spark.sql("create database learning")
#spark.sql("show databases").show()
spark.sql("use learning")

'''ASSIGNMENT 1: Latest Record per Key (SCD-Type Scenario)
spark.sql("""CREATE TABLE IF not EXISTS employee_history (
  emp_id INT,
  salary INT,
  updated_ts STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
""")
spark.sql("""
INSERT INTO employee_history VALUES
(1, 50000, '2023-01-01'),
(1, 60000, '2024-01-01'),
(1, 65000, '2024-06-01'),
(2, 70000, '2023-05-01'),
(2, 75000, '2024-02-01')
""")
spark.sql("select emp_id, salary, updated_ts from (select *, ROW_NUMBER() over(partition by emp_id order by updated_ts desc) as rn from employee_history) tbl where rn = 1").show()
'''

'''Hive Assignment 1: Employee Salary Analysis'''

'''spark.sql("""CREATE TABLE if not exists employee (
    emp_id INT,
    emp_name STRING,
    dept_id INT,
    salary FLOAT,
    join_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;""")
# spark.sql("""
# INSERT INTO employee VALUES
# (1, 'John', 10, 50000, '2020-01-10'),
# (2, 'Alice', 20, 60000, '2019-03-15'),
# (3, 'Bob', 10, 45000, '2021-07-21'),
# (4, 'David', 30, 70000, '2018-05-11'),
# (5, 'Eva', 20, 65000, '2022-02-01'),
# (6, 'Frank', 10, 48000, '2023-01-05');
# """)'''
spark.sql("select * from employee")
# 1. Average salary per department
spark.sql("select dept_id, avg(salary) as avg_sal from employee group by dept_id")
spark.sql("select *,avg(salary) over(partition by dept_id) as avg_sal from employee")
#2. Top 2 highest salaries in each department
spark.sql("select emp_id, emp_name, dept_id, salary, join_date from "
          "(select *, Dense_rank() over(partition by dept_id order by salary desc) as d_rnk from employee) tbl where d_rnk <= 2")
#3. Employees above overall average salary
spark.sql("select * from employee where salary > (select avg(salary) as overall_avg from employee)")

#4. Calculate the salary difference from the department average for each employee
spark.sql("select e.emp_id,e.emp_name,e.dept_id,e.join_date, e.salary,a.dep_avg_sal , e.salary-dep_avg_sal as diff from employee e join (select dept_id, avg(salary) as dep_avg_sal from employee group by dept_id) a on "
          "e.dept_id = a.dept_id")

spark.sql("select e.emp_id,e.emp_name,e.dept_id,e.join_date, e.salary, e.salary - avg(salary) over(partition by dept_id) as diff from employee e")

'''Hive Assignment 2: Sales Data Analysis with Partitions'''

'''spark.sql("""CREATE TABLE sales (
    sale_id INT,
    product STRING,
    category STRING,
    amount FLOAT,
    sale_date STRING
)
PARTITIONED BY (year INT, month INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;""")

spark.sql("""INSERT INTO TABLE sales PARTITION (year=2023, month=1) VALUES
(1, 'Laptop', 'Electronics', 1200, '2023-01-05'),
(2, 'Mouse', 'Electronics', 25, '2023-01-15');""")

spark.sql("""INSERT INTO TABLE sales PARTITION (year=2023, month=2) VALUES
(3, 'Keyboard', 'Electronics', 45, '2023-02-01'),
(4, 'Chair', 'Furniture', 150, '2023-02-10');""")'''

spark.sql("select * from sales").show()

'''Calculate total sales amount per category per month.'''
spark.sql("select *, sum(amount) over(partition by category,month) as tot_sal_cat_month from sales").show()
'''Find the month with highest sales per category.'''
spark.sql("select *, max(amount) over(partition by category, month) as highest_sale from sales").show()
spark.sql("select *, sum(amount) as tot_amount, row_number() over("
          "partition by category order by sum(amount) desc) as rn from sales").show()

#select sale_id, product,   category,amount, sale_date,year,month,