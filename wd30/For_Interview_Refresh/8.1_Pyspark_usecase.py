import os
os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\user\\AppData\\Local\\Programs\\Python\\Python311\\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark=SparkSession.builder.master("local[*]").appName("use_case_1").getOrCreate()

data=[
    (1,"Gokul", 3000, None),
    (2,"Panner", 2000, 1),
    (4,"Neha", 2500, 2),
    (5, "Vivek", 900, 2),
    (3, "Ponraj", 800, 1),
    (6, "Uday", 950, 3)
]

employee=spark.createDataFrame(data, ["id","name","salary","reporting_manager_id"])
employee.orderBy(col("id")).show()

emp=employee.alias("e")
mgr=employee.alias("m")

emp_mgr= emp.join(mgr, col("e.reporting_manager_id") == col("m.id"), how="inner")
emp_mgr.filter(col("e.salary") > col("m.salary")).\
select("e.id", "e.name", "e.salary", "e.reporting_manager_id").show()



