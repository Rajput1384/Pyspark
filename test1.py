from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSV Practice").getOrCreate()

df = spark.read.csv(
    r"C:\Users\arvin\OneDrive\Desktop\Pyspark\employee.csv",
    header=True,
    inferSchema=True
)

#df.show()
#df.printSchema()

#df.createOrReplaceTempView("employees")

#Q1 — Show all employees from IT department
#df.filter(df.department=="IT").show()

#Q2 — Employees with salary > 60000
#df.filter(df.salary > 60000).show()

#Q3 — Select only name and city
#df.select("name","city").show()

#Q4 — Average salary by department (IMPORTANT)
#from pyspark.sql.functions import avg
#df.groupBy("department").agg(avg("salary")).show()

#Q5 — Count employees per city
#df.groupBy("city").count().show()

#Q6 — Highest salary employee
#df.orderBy(df.salary.desc()).show(1)

#Q7 — Employees with experience > 3 years
#df.filter(df.experience > 3).show()

#Q8 — Add Bonus Column (15%)
#from pyspark.sql.functions import col
#df.withColumn("bonus",col("salary")*0.15).show( )

#Q9 — Salary Category Column
from pyspark.sql.functions import when
from pyspark.sql.functions import col

df.withColumn(
    "grade",
    when(col("salary") >= 65000,"High")
    .when(col("salary") >= 50000,"Medium")
    .otherwise("Low")
).show()

#Q10 — Department-wise Max Salary
from pyspark.sql.functions import max

df.groupBy("department") \
  .agg(max("salary").alias("max_salary")) \
  .show()


#Q11 — Top 3 Highest Paid Employees
df.orderBy(col("salary").desc()).limit(3).show()

#Q12 — Employees Age Between 25–30
df.filter((col("age") >= 25) & (col("age") <= 30)).show()

#Q13 — Sort by Experience
df.orderBy("experience").show()

#Q14 — Write Output to CSV (Real Job Task)
df.write.mode("overwrite").csv("output_employees", header=True)