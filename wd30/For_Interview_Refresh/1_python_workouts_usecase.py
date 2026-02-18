import os
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"

employees = [
    {"id": 1, "name": "Alice", "salary": 3000, "manager_id": None},
    {"id": 2, "name": "Bob", "salary": 2000, "manager_id": 1},
    {"id": 4, "name": "David", "salary": 2500, "manager_id": 2},
    {"id": 5, "name": "Eve", "salary": 900, "manager_id": 2},
    {"id": 3, "name": "Carol", "salary": 800, "manager_id": 1},
    {"id": 6, "name": "Frank", "salary": 950, "manager_id": 3}
]

emp_dict={emp["id"]: emp for emp in employees}
result = []
print(emp_dict)
for emp in employees:
    print(emp)
    manager_id = emp["manager_id"]
    #if manager_id is None:
      #  continue
    print(manager_id)
    manager = emp_dict.get(manager_id)
    print(f"manager's records fetched from emp_dict using the '{manager_id}' : ", manager)
    print("\n")
    if manager_id and emp["salary"] > manager["salary"]:
        result.append(emp)

print("Final Result: " ,result)
print("Employees earning more than their manager:\n")

for r in result:
    print(
        f"ID: {r['id']}, "
        f"Name: {r['name']}, "
        f"Salary: {r['salary']}, "
        f"Manager ID: {r['manager_id']}"
    )