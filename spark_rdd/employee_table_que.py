from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import expr

from pyspark.sql.types import *

if __name__ == '__main__':


    spark = SparkSession.builder\
                        .master("local[*]")\
                        .appName("Mock_Practice") \
                        .config("spark.driver.bindAddress", "localhost") \
                        .config("spark.Ui.port", "4040") \
                        .getOrCreate()

    emp_df = spark.read.csv(r"D:\Apache Spark Data\spark_files\EMP_DETAILS.csv",inferSchema=True,header=True)
    emp_df.show()

    incentive_df = spark.read.csv(r"D:\Apache Spark Data\spark_files\incentive.csv",inferSchema=True,header=True)
    incentive_df.show()

    # Q.1 Get First_Name,Last_Name from employee table

    df1 = emp_df.select("First_name", "Last_name")
    #df1.show()


    # Q.2 Get First_Name from employee table using alias name “Employee Name”

    df2 = emp_df.select(col("First_name").alias("Employee Name"))
    #df2.show()


    # Q.3 Get First_Name from employee table in upper case

    df3 = emp_df.select("First_name".upper())
    #df3.show()


    # Q.4 Select first 3 characters of FIRST_NAME from EMPLOYEE

    df4 = emp_df.select(col("First_name").substr(1, 3))
    #df4.show()


    # Q.5 Get position of 'o' in name 'John' from employee table

    df5 = emp_df.select("First_name").withColumn('pos', F.instr(emp_df["First_name"],'o'))
    #df5.show()

    # Q.6 Get FIRST_NAME from employee table after removing white spaces from right side.

    df6 = emp_df.select("First_name").withColumn("rtrim_word",rtrim(col("First_name")))
    #df6.show()


    # Q.7 Get First_Name from employee table after replacing 'o' with '$'

    df7 = emp_df.select("First_name").na.replace("John","Komal")
   #df7.show()

    df8 = emp_df.select("First_name").withColumn("Replace_word",translate("First_name",'o','$'))
    #f8.show()


    # Q.8 Get First_Name and Last_Name as single column from employee table separated by a '_'

    df9 = emp_df.select("First_name","Last_name")\
                        .withColumn("Concat_col",concat(col("First_name"),lit("_"),col("Last_name")))
    #df9.show()

    df = emp_df.withColumn("Full_name",expr("First_name ||' '|| Last_name"))
    #df.show()

    # Q.9 Get FIRST_NAME, Joiningyear, Joining Month and Joining Date from employee table

    inputdf = emp_df.select(col("Joining_date").cast("date"))
    inputdf.printSchema()
    df10 = inputdf.select("Joining_date",year(col("Joining_date")).alias("year"),
                         month(col("Joining_date")).alias("month"),
                         dayofmonth(col("Joining_date")).alias("day"))
    #df10.show()


    # Q.11 Get all employee details from the employee table order by First_Name Ascending

    df11 = emp_df.select("*").orderBy(col("First_name").asc())
    #df11.show()

    # Q.12 Get all employee details from the employee table order by First_Name Ascending and Salary Descending?

    df12 = emp_df.orderBy(col("First_name").asc()).orderBy(col("Salary").desc())
    #df12.show()

    # Q.13 Get employee details from employee table whose employee name is “John”

    df13 = emp_df.filter("First_name == 'John'")
    #df3.show()

    df14 = emp_df.where("First_name == 'John'")
    #df14.show()

    # Q.14 Get employee details from employee table whose employee name are “John” and “Roy”

    df15 = emp_df.where("First_name == 'John' or First_name == 'Roy'")
    #df15.show()


    df16 = emp_df.filter("First_name == 'John' or First_name == 'Roy'")
    #df16.show()

    # Q.15 Get employee details from employee table whose employee name are not “John” and “Roy”.

    df17 = emp_df.where("First_name != 'John' and First_name != 'Roy'")
    #df17.show()

    df18 = emp_df.filter("First_name != 'John' and First_name != 'Roy'")
    #df18.show()

    # Q.16 Get employee details from employee table whose first name starts with 'J'.

    df19 = emp_df.filter(emp_df.First_name.startswith("J"))
    #df19.show()

    df20 = emp_df.where(emp_df.First_name.startswith("J"))
    #df20.show()

    # Q.17 Get employee details from employee table whose first name contains 'o'.

    df21 = emp_df.filter(emp_df.First_name.contains("o"))
    #df21.show()

    # Q.18 Get employee details from employee table whose first name ends with 'n'.

    df22 = emp_df.where(emp_df.First_name.endswith("n"))
    #df22.show()

    # Q.19 Get employee details from employee table whose first name ends with 'y' and firstname contains r letters.

    df23 = emp_df.filter(emp_df.First_name.endswith("y") | emp_df.First_name.contains("R"))
    #df23.show()

    # Q.20 Get employee details from employee table whose first name starts with 'J' and name contains 4 letters.

    df24 = emp_df.where(emp_df.First_name.startswith("J"))
    #df24.show()

    # Q.21 Get employee details from employee table who’s Salary greater than 600000.

    df25 = emp_df.filter("Salary > 600000")
    # df25.show()

    # Q.22 Get employee details from employee table whose joining year is “2013”.

    df77 = emp_df.select(col("Joining_date").cast("date"))
    df77.printSchema()
    df26 = df77.select("Joining_date",year(col("Joining_date")).alias("year"))\
                      .where("year = 2013")
    #df26.show()

    # Q.23 Get department, total salary with respect to a department from employee table.

    df27 = emp_df.groupBy("Department").agg(sum("Salary"))
    #df27.show()

    # Q.24 Get department, total salary with respect to a department from employee table order by total salary descending.

    df28 = emp_df.groupBy("Department").agg(sum("Salary").alias("sum_sal")).sort(desc("sum_sal"))
    # df28.show()

    # Q.25 Get department, no of employees in a department, total salary with respect to a department from employee
    # table order by total salary descending.

    # df29 = emp_df.agg(count("Department").alias("No_of_emp")).groupBy("Department")\
    #                         .agg(sum("Salary").alias("sum_sal")).sort(desc("sum_sal"))
                    # .agg(count("emp_id").alias("no_of_emp"))
    #df29.show()


    # Q.26 Get department wise average salary from employee table order by salary ascending?

    df30 = emp_df.groupBy("Department").agg(avg("Salary").alias("Avg_sal")).sort(asc("Avg_sal"))
    #df30.show()


    # Q.27 Get department wise maximum salary from employee table order by salary ascending?

    df31 = emp_df.groupBy("Department").agg(max("Salary").alias("Max_sal")).sort(asc("Max_sal"))
    #df31.show()

    # Q.28 Get department wise minimum salary from employee table order by salary ascending?

    df32 = emp_df.groupBy("Department").agg(min("Salary").alias("Min_sal")).sort(asc("Min_sal"))
    #df32.show()

    # Q.29 Select no of employees joined with respect to year and month from employee table.

    df99 = emp_df.select("EMPNAME")

    # Q.30 Select department,total salary with respect to a department from employee table where total salary greater
    # than 800000 order by Total_Salary descending

    # df33 = emp_df.groupBy("Department").agg(sum("Salary")).alias("sum_sal")\
    #                 .where(col("sum_sal") > 800000).sort(desc("Sum_sal"))
    # df33.show()


    # Q.31 Select employee details from employee table if data in incentive table?

    df34 = emp_df.join(incentive_df,on=emp_df.Emp_id==incentive_df.Employee_ref_id,
                                    how='inner').select((["Emp_id","First_name","Last_name",
                                                          incentive_df.Employee_ref_id]))
    #df34.show()

    # Q.32 Select 20 % of salary from John, 10% of Salary for Roy and for other 15 % of salary from employee table



    # Q.33 Select Banking as 'Bank Dept',Insurance as 'Insurance Dept' and Services as 'Services Dept' from employee table.

    # df36 = emp_df.select("Department").withColumn("Replace_word",regexp_replace(col("Department"),"Banking","BankDept"))\
    #                 .withColumn("Replace_word1",regexp_replace(col("Department"), "Insurance", "Insurance Dept"))\
    #                 .withColumn("Replace_word2",regexp_replace(col("Department"), "Services", "Services Dept"))
    # df36.show()

    df37 = emp_df.na.replace("Banking","Bank Dept").replace("Insurance","Insurance Dept")\
                        .replace("Services", "Services Dept")
   # df37.show()


    # Q.34 Delete employee data from employee table who got incentives in incentive table

    df39 = emp_df.join(incentive_df,on=emp_df.Emp_id==incentive_df.Employee_ref_id,
                                   how='left_anti')
    #df39.show()

    # Q.35 Select first_name, incentive amount from employee and incentives table for those employees who have incentives.

    df40 = emp_df.join(incentive_df, on=emp_df.Emp_id==incentive_df.Employee_ref_id,
                                    how='inner').select(["First_name",incentive_df.Incentive_amount])
    #df40.show()

    # Q.36 Select first_name, incentive amount from employee and incentives table for those employees who have incentives
    # and incentive amount greater than 3000

    df41 = emp_df.join(incentive_df, on=emp_df.Emp_id == incentive_df.Employee_ref_id,
                       how='inner').select(["First_name", incentive_df.Incentive_amount])\
                        .filter("Incentive_amount > 3000")
    #df41.show()


    # Q.36 Select first_name, incentive amount from employee and incentives table for all employees
    # even if they didn't get incentives

    df42 = emp_df.join(incentive_df, on=emp_df.Emp_id == incentive_df.Employee_ref_id,
                       how='left').select(["First_name", incentive_df.Incentive_amount])
    #df42.show()

    # Q.37 Select first_name, incentive amount from employee and incentives table for all employees even if they didn't get
    # incentives and set incentive amount as 0 for those employees who didn't get incentives.---Error

    df43 = emp_df.join(incentive_df, on=emp_df.Emp_id == incentive_df.Employee_ref_id,
                       how='left').select(["First_name",incentive_df.Incentive_amount])

                                   # .fillna({'incentive_df.Incentive_amount':0})
    #df43.show()

    # Q.38 Select first_name, incentive amount from employee and incentives table for all employees who got
    # incentives using left join

    df44 = emp_df.join(incentive_df, on=emp_df.Emp_id == incentive_df.Employee_ref_id,
                       how='right').select(["First_name", incentive_df.Incentive_amount])
    #df44.show()


    # Q.39 Select max incentive with respect to employee from employee and incentives table using sub query.

    df45 = emp_df.join(incentive_df, on=emp_df.Emp_id == incentive_df.Employee_ref_id,
                       how='inner').select(["First_name", incentive_df.Incentive_amount])\
                        .agg(max("incentive_amount").alias("max_val"))
    #df45.show()