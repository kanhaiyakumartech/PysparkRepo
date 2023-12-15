from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, expr, current_date

def create_employee_df(spark):
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("state", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("age", IntegerType(), True)
    ])

    employee_data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    return spark.createDataFrame(employee_data, schema=employee_schema)

def create_department_df(spark):
    department_schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])

    department_data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    return spark.createDataFrame(department_data, schema=department_schema)

def create_country_df(spark):
    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])

    country_data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]

    return spark.createDataFrame(country_data, schema=country_schema)
    
#2. Here i defined a functions to Find  avg salary of each departme
def avg_salary_per_department(employee_df):
    return employee_df.groupBy("department").agg(expr("avg(salary) as avg_salary"))
    
#3.Here i Defined a function to Find the employee name and department name whose name starts with ‘m’ 
def employee_name_department_starts_with_m(employee_df):
    return employee_df.filter(col("employee_name").startswith("m")).select("employee_name", "department")
    

# 4.Here i use withColumn with conditions to create another new column in  employee_df as bonus by multiplying employee salary *2
def add_bonus_column(employee_df):
    return employee_df.withColumn("bonus", col("salary") * 2)
    
#5. reorder the column names of employee_df columns  as (employee_id,employee_name,salary,State,Age,department
def reorder_columns(employee_df):
    return employee_df.select("employee_id", "employee_name", "salary", "state", "age", "department")
    
#6.Give the result of inner join, left join, right join when joining employee_df with department_df in dynamic way
def join_dataframes(employee_df, department_df, join_type):
    return employee_df.join(department_df, employee_df.department == department_df.dept_id, how=join_type)
    
#7.derive a new dataframe with country_name instead of State in employee_df :- Eg(11,“james”,”D101”,”newyork”,8900,32)
def replace_state_with_country_name(employee_df, country_df):
    return employee_df.join(country_df, employee_df.state == country_df.country_code, "left").drop("state").withColumnRenamed("country_name", "state")
    

#8.convert all the column names into lower case from the result of question 7in dynamic way, add load_date column with current date
def convert_column_names_to_lower_case(employee_df):
    return employee_df.toDF(*[col.lower() for col in employee_df.columns])


#9.create 2 external tables with parquet,csv format with same name database name and 2 different table names as csv and parquet format.
def add_load_date_column(employee_df):
    return employee_df.withColumn("load_date", current_date())

