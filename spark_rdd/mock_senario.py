from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import expr
from pyspark.sql.window import Window

from pyspark.sql.types import *

if __name__ == '__main__':


    spark = SparkSession.builder\
                        .master("local[*]")\
                        .appName("Mock_Practice") \
                        .config("spark.driver.bindAddress", "localhost") \
                        .config("spark.Ui.port", "4040") \
                        .getOrCreate()

    # Q.1
    #way1
    # df = spark.read.csv("D:\GCP\spark_practice_project\input_file\input_data.csv",header=True)
    # df.show()
    #
    # df2 = df.groupBy().pivot("column_name").agg(first("value"))
    # df2.show()
    #
    # #way2
    # data =[("col1", "val1"),
    #         ("col2", "val2"),
    #         ("col3", "val3"),
    #         ("col4", "val4"),
    #         ("col5", "val5")]
    #
    # rdd =spark.sparkContext.parallelize(data)
    # print(rdd.collect())
    #
    # df1 = rdd.toDF(["COLUMN_NAME", "VALUE"])
    # print(df1.show())
    #
    # df2 = df1.groupBy().pivot("column_name").agg(first("value"))
    # df2.show()


    # Q.2
    # df1 = spark.read.csv("D:\GCP\spark_practice_project\input_file\input_data1.csv",header=True)
    # df1.show()
    #
    # # way1
    # df3 = df1.withColumn("concat_records", concat_ws("",df1.col1,df1.col2,df1.col3))
    # df3.show()
    #
    # # way2
    # df4 = df1.withColumn("concat", concat(coalesce(df1.col1, lit('')), coalesce(df1.col2, lit('')), coalesce(df1.col3, lit(''))))
    # df4.show()


    # Q.3

    # from itertools import chain
    # from pyspark.sql import DataFrame
    #
    #
    # def _sort_transpose_tuple(tup):
    #     x, y = tup
    #     return x, tuple(zip(*sorted(y, key=lambda v_k: v_k[1], reverse=False)))[0]
    #
    #
    # def transpose(X):
    #     """Transpose a PySpark DataFrame.
    #
    #     Parameters
    #     ----------
    #     X : PySpark ``DataFrame``
    #         The ``DataFrame`` that should be tranposed.
    #     """
    #     # validate
    #     if not isinstance(X, DataFrame):
    #         raise TypeError('X should be a DataFrame, not a %s'
    #                         % type(X))
    #
    #     cols = X.columns
    #     n_features = len(cols)
    #
    #     # Sorry for this unreadability...
    #     return X.rdd.flatMap(  # make into an RDD
    #         lambda xs: chain(xs)).zipWithIndex().groupBy(  # zip index
    #         lambda val_idx: val_idx[1] % n_features).sortBy(  # group by index % n_features as key
    #         lambda grp_res: grp_res[0]).map(  # sort by index % n_features key
    #         lambda grp_res: _sort_transpose_tuple(grp_res)).map(  # maintain order
    #         lambda key_col: key_col[1]).toDF()  # return to DF
    #
    #
    # x =spark.sparkContext.parallelize([(1,2,3),(4,5,6),(7,8,9)]).toDF()
    # x.show()

    #df12 = transpose(x).show()


    # df222 = x.select("col1",explode(x._1))
    # df222.show()

    # Q.4


    # | num1 | num2
    # | 1 | a / b |
    # | 2 | a |
    # | 3 | a / b / c |

    # +----+---+
    # | num1 | col |
    # +----+---+
    # | 1 | a |
    # | 1 | b |
    # | 2 | a |
    # | 3 | a |
    # | 3 | b |
    # | 3 | c |
    # +----+---+

    # rdd1 = spark.sparkContext.parallelize([(1,"a/b"),(2,"a"),(3,"a/b/c")])
    # print(rdd1.collect())
    #
    # df13 = rdd1.toDF(["num1","num2"])
    # df13.show()
    #
    # df21 = df13.select("num1", split(col("num2"), "/",-1).alias("split_string"))
    # df21.show()
    #
    # df14 = df21.select("num1", explode(df21.split_string))
    # df14.show()


    # Q.5  what will be inner join,left outer join,right outer join
    #  tab1 tab2
    #  1     2
    #  2     1
    #  2     2
    #  1     1

    # df15 = spark.read.csv(r"D:\Apache Spark Data\spark_files\tab1.txt",header=True)
    # df15.show()
    #
    # df16 = spark.read.csv(r"D:\Apache Spark Data\spark_files\tab2.txt",header=True)
    # df16.show()
    #
    # res_df = df15.join(df16, on=df15.id==df16.id, how='inner')
    # res_df.show()
    #
    # res_df1 = df15.join(df16, on=df15.id == df16.id, how='left')
    # res_df1.show()
    #
    # res_df2 = df15.join(df16, on=df15.id == df16.id, how='right')
    # res_df2.show()
    #
    # res_df3 = df15.join(df16, on=df15.id == df16.id, how='left_anti')
    # res_df3.show()



    # Q.6 Input-----------------ERROR
    # T1 (1,2,3,4,5,6,7)
    # T2 (a,b,c,d)

    # Output
    # col1 col2
    # 1     a
    # 2     b
    # 3     c
    # 4     d
    # 5
    # 6
    # 7
    #
    # df200 = spark.createDataFrame([(1,),(2,),(3,),(4,),(5,),(6,),(7,)],["col1"])
    # df200.show()
    #
    # df201 = spark.createDataFrame([("a",),("b",),("c",),("d",)],["col2"])
    # df201.show()
    #
    # joindf = df200.join(df201, df200.col1==df201.col2, how="left").withColumn("rno",row_number().over(windwspace))
    # joindf.show()




    # windowspace = Window.orderBy("col1")
    # windowspace1 = Window.orderBy("col2")
    # df3 = df200.withColumn("row_number", row_number().over(windowspace))
    # df4 = df201.withColumn("row_number", row_number().over(windowspace1))
    # df5 = df3.join(df4, df3.row_number == df4.row_number, 'leftouter').select(df3['COL1'], df4['col2']).show()




    # Q.7 Join word of two colums into single column
    # I/P= col1:Komal col2:Patil  O/P= col1:Komal Patil

    # data1 =[("Komal","patil")]
    #
    # rdd5 = spark.sparkContext.parallelize(data1)
    # print(rdd5.collect())
    #
    # df17 = rdd5.toDF(["First_name","Last_name"])
    # print(df17.show())
    #
    # df18 = df17.withColumn("Full_name",concat(col("First_name"),lit(" "),col("Last_name")))
    # df18.show()
    #
    # df19 = df17.withColumn("Full_name", expr("First_name ||' '|| Last_name"))
    # df19.show()



    # Q.8
    # id,name,dept,sal,gender
    # 1,vaibhav,IT,1000,m
    # 2,Ketan,fin,2000,m
    # 3,Saloni,Hr,3000,f
    # 4,Shreya,IT,4000,f
    # 5,sumit,Hr,2000,m
    # 6,sejal,fin,2000,f
    #
    # Count gender with respect to id in sql and dataframe
    # o/p m =3, f = 3

    # df20 = spark.read.csv(r"D:\Apache Spark Data\spark_files\empl.txt",header=True)
    # df20.show()
    #
    # df21 = df20.groupBy("gender").agg(count(col("gender")).alias("Gender_cnt"))
    # df21.show()


    # Q.9 input=komalpatil
    # output
    # k
    # o
    # m
    # a
    # l
    # p
    # a
    # t
    # i
    # l


    # data2 = [("komalPatil","poojaAtkar")]
    #Using RDD
    # rdd6 = spark.sparkContext.parallelize(data2)
    # print(rdd6.collect())
    #
    # df21 = rdd6.toDF(["col1","col2"])
    # print(df21.show())

    # df22 = df21.select(split(col("col1"), "").alias("split_col1"), split(col("col2"), "").alias("split_col2"))
    # df22.show()
    #
    # df23 = df22.select(explode("split_col1").alias("explode_col1"))
    # df23.show()

    #using DF
    # df21 = spark.createDataFrame([("komalPatil",)],["col1"])
    # df21.show()
    #
    # df22 = df21.select(split(col("col1"),"").alias("split_col1"))
    # df22.show()
    #
    # df23 = df22.select(explode("split_col1").alias("explode_col1"))
    # df23.show()


    # Q.10 what will output of tbl1 union tbl2 and tbl1 union all tbl2?
    # tbl1         #tbl2
    # 1              1
    # 2              2
    # 3              3
    # 4              4

    #
    # df24 = spark.read.csv(r"D:\Apache Spark Data\spark_files\table1.txt",header=True)
    # df24.show()
    #
    # df25 = spark.read.csv(r"D:\Apache Spark Data\spark_files\table2.txt",header=True)
    # df25.show()
    #
    # df26 = df24.union(df25)
    # df26.show()
    #
    # df27 = df24.unionAll(df25)
    # df27.show()


    # Q.11	Remove duplicates from following table using Window function(row_number)

    #  tab1
    #  1
    #  2
    #  2
    #  1

    # df28 = spark.read.csv(r"D:\Apache Spark Data\spark_files\duplicate_data.txt",header=True)
    # df28.show()
    # df28.printSchema()

    #change dataType
    # df29 = df28.select(col("id").cast("int").alias("Change_dataType"))
    # df29.show()
    # df29.printSchema()

    #Drop_duplicate_value
    # df30 = df29.orderBy("Change_dataType").drop_duplicates(subset=["Change_dataType"])
    # df30.show()


    # Q.12	Write Word Count program in RDD.

    # rdd7 = spark.sparkContext.textFile(r"C:\Users\Administrator\Downloads\sample1.txt")
    # print(rdd7.collect())
    #
    # rdd8 = rdd7.flatMap(lambda x:x.split(" "))
    # print(rdd8.collect())
    #
    # rdd9 = rdd8.map(lambda x: (x, 1))
    # print(rdd9.collect())
    #
    # rdd10 = rdd9.reduceByKey(lambda x,y: x+y)
    # print(rdd10.collect())

    # lines = spark.read.text(r"C:\Users\Administrator\Downloads\sample1.txt")
    # lines.createTempView("word_file")
    # wordsDF = spark.sql("select explode(split(value, ' ')) as word from word_file")
    # wordsDF.show(5)
    #
    # wordsDF.createTempView("word_file1")
    # wordCountDF = spark.sql("select word,count(word) wordcount from word_file1 group by word")
    # wordCountDF.show()



    # Q.13 Split word into two colums
    # NAME
    # TEAM BRAINWORKS

    #using df
    # df111 = spark.createDataFrame([("TEAM BRAINWORKS",)],["Narhe"])
    # df111.show()

    # df222 = df111.select(split(col("Narhe")," ").getItem(0).alias("col1"),
    #                 split(col("Narhe")," ").getItem(1).alias("col2"))
    # df222.show()

    #using rdd
    # data3 = [("TEAM BRAINWORKS","Qspiders")]
    #
    # rdd11 = spark.sparkContext.parallelize(data3)
    # print(rdd11.collect())
    #
    # df31 = rdd11.toDF(["Narhe","Wakad"])
    # print(df31.show())

    #using rdd
    # df32 = df111.select(split("Narhe", " ")).rdd.flatMap(lambda x : x).toDF(schema=["col1","col2"])
    # df32.show()




    # Q.14 Add one column City having Pune as value.
    # 1 ST
    # 2 AB
    # 2 AB
    # 3 VP
    # 4 GH
    #OR#
    # Write a syntax for add new column and new values under same column in spark ?

    # inputdata1 = ([(1,"SA"),(2,"AB"),(2,"AB"),(3,"VP"),(4,"GH")])
    #
    # rdd8 = spark.sparkContext.parallelize(inputdata1)
    # print(rdd8.collect())
    #
    # df31 = rdd8.toDF(["id","name"])
    # print(df31.show())
    #
    # df32 = df31.withColumn("city", lit("pune"))
    # df32.show()


    # Q.15 Replace all values in city column with only 'Pune'

    # inputdata2 = ([(1,"komal","latur"),(2,"raj","Akola"),(2,"neha","Gulbarga"),(3,"priya","udgir"),(4,"vishal","bidar")])
    #
    # rdd9 = spark.sparkContext.parallelize(inputdata2)
    # print(rdd9.collect())
    #
    # df33 = rdd9.toDF(["id","name","city"])
    # print(df33.show())
    #
    # df34 = df33.withColumn("city",lit("pune"))
    # df34.show()


    # Q.16 Write program to find 3rd highest salary using sql query and using data frame.

    # emp_df = spark.read.csv(r"D:\Apache Spark Data\spark_files\EMP_DETAILS.csv", inferSchema=True,header=True)
    # emp_df.show()
    #
    # windospace = Window.orderBy(desc_nulls_last("Salary"))
    #
    # df35 = emp_df.withColumn("highest_sal", row_number().over(windospace)).filter("highest_sal = 3")
    # df35.show()


    # Q.17 list=(1,2,3,4)
    # output
    #    1
    #    2
    #    3
    #    4

    #way1
    # dff = spark.createDataFrame([("1","2","3","4")],["col1","col2","col3","col4"])
    # dff.show()

    # df113 = dff.withColumn("concat_records", concat_ws(",", dff.col1,dff.col2,dff.col3,dff.col4))
    # df113.show()
    #
    # df114 = df113.withColumn("output", explode(split("concat_records",",")))
    # df114.show()

    #way2
    # df = spark.sparkContext.parallelize([(1, 2, 3, 4)
    #                     ]).toDF(['col1', 'col2', 'col3', 'col4'])
    # print(df.collect())
    #
    # df1 = df.withColumn("concat_records", concat_ws(",", df.col1,df.col2,df.col3,df.col4))
    # df1.show()
    #
    # df2 = df1.withColumn('final_result', explode(split('concat_records', ','))).show()
    # df2.show()


    # Q.18 tb1   tab2
    #       1     2
    #       2     3
    #       3     6
    #       4     5
    #       7

    # list1 = spark.sparkContext.parallelize([1,2,3,4,7])
    # print(list1.collect())
    #
    # list2 = spark.sparkContext.parallelize([2,3,6,5])
    # print(list2.collect())
    #
    # result = list1.intersection(list2)
    # print(result.collect())

    # Q.19 +-------------+-------------+
    # |Department_No|Employee_Name|
    # +-------------+-------------+
    # |           20|            R|
    # |           10|            A|
    # |           10|            D|
    # |           20|            P|
    # |           10|            B|
    # |           10|            C|
    # |           20|            Q|
    # |           20|            S|
    # +-------------+-------------+
    # Desired output
    # +-------------+------------+
    # |Department_No|    emp_name|
    # +-------------+------------+
    # |           10|[A, D, B, C]|
    # |           20|[R, P, Q, S]|
    # +-------------+------------+


    # df55 = spark.createDataFrame([(20,"R"),(10,"A"),(10,"D"),(20,"P"),(10,"B"),(10,"C"),(20,"Q"),(20,"S")],["Department_No","Employee_Name"])
    # df55.show()
    #
    # df56 = df55.groupBy(col("Department_No")).agg(collect_list("Employee_Name").alias("emp_name")).orderBy(col("emp_name").asc())
    # df56.show()



    # Q.20-----
    #   input     output
    #     1         1
    #     1         0
    #     1         1
    #     1         0
    #     0         1
    #     0         0
    #     0         1
    #     0         0


    # df57 = spark.createDataFrame([(1,),(1,),(1,),(1,),(0,),(0,),(0,),(0,)],["input"])
    # df57.show()
    #
    # windowspace = Window.partitionBy("input").orderBy(col("input").asc())
    #
    # df58 = df57.withColumn("Output",row_number().over(windowspace))
    # # df58.show()
    # df58.orderBy("output").show()



    # Q.21
    # input= Emp_Table
    #  id  name mgrid
    #  1    a     2
    #  2    b     3
    #  3    c    null
    # Write a query to get manager name of each of the employee?
    # Expected output
    # empName  mgrName
    #    a       b
    #    b       c
    #    c      null


    # df59 = spark.sparkContext.parallelize([(1,"a",2),(2,"b",3),(3,"c","null")]).toDF(["id","name","mgrid"])
    # df59.show()
    #
    # df60 = df59.select("name").withColumnRenamed("name","empName")
    # df60.show()
    #
    # df61 = df60.withColumn("mgrName", when(col("empName").isin("a"),"b").otherwise(when(col("empName").isin("b"),"c")\
    #                         .otherwise(when(col("empName").isin("c"),"null").otherwise("null"))))
    # df61.show()


    # Q.22
    # input=  Gender        output=  Gender  Category
    #          Male                   Male      M
    #         Female                 Female     F


    # df110 = spark.createDataFrame([("Male",),("Female",)],["Gender"])
    # df110.show()
    #
    # # way1
    # df120 = df110.withColumn("Category", when(col("Gender") == "Male", "M")\
    #                 .when(col("Gender") == "Female", "F").otherwise("Unknown"))
    # df120.show()

    #way2
    # def fullformfunc(element):
    #     if str(element).upper() == "MALE":
    #         output = "M"
    #     elif str(element).upper() == "FEMALE":
    #         output = "F"
    #     else:
    #         output = "other"
    #     return output
    #
    # fullformUDF = udf(lambda x: fullformfunc(x))
    #
    # df110.select("Gender", fullformUDF(col("Gender")).alias("Gender_new"))\
    #     .drop("Gender").show()

    #way2
    # df120 = df110.withColumn("Category", when(col("Gender") == "Male", "M")\
    #                 .when(col("Gender") == "Female", "F").otherwise("Unknown"))
    # df120.show()



    # Q.23 Input = Fname  Lname         output=  ShortName
    #              John   Torato                    JT
    #              Jack   Ley                       JL


    # df123 = spark.createDataFrame([("John","Torato"),("Jack","Ley")],["Fname","Lname"])
    # df123.show()
    #
    # df124 = df123.withColumn("col1",substring("Fname",1,1)).withColumn("col2",substring("Lname",1,1))
    # df124.show()
    #
    # df125 = df124.withColumn("ShortName",concat_ws("",df124.col1,df124.col2))
    # df125.show()



    # Q.24

    # data = [("'amruta', 'girish', 'kadam'", 1000, 'F'), ("'dhiraj', 'shivaji', 'SHINDE'", 1000, 'M'),
    #         ("'komal', '', 'patil'", 1000, 'M')]
    #
    #
    # columns = ["name","salary","gender"]
    # df1 = spark.createDataFrame(data=data, schema=columns)
    # df1.show(truncate=False)
    # df1.printSchema()
    #
    # df2 = df1.select(split(col("name"),",").getItem(0).alias("nam1")),split(col("name"),",").getItem(1).alias("nam2"),
    #             split(col("name"),",").getItem(2).alias("nam3")
    # df2.show()


    # df = spark.createDataFrame([(('amruta', "girish", "kadam"), 1000, 'F'), (('dhiraj', 'shivaji', 'SHINDE'), 1000, 'M'),
    # (('komal', 'shahaji', "patil"), 1000, 'M')],["name","salary","gender"])
    # df.show(truncate=False)
    #
    # df1 = df.select(split('name', ",")).alias("nm1")
    # df1.show()

    # data = [(('amruta', "girish", "kadam"), 1000, 'F'), (('dhiraj', 'shivaji', 'SHINDE'), 1000, 'M'),
    #         (('komal', '', "patil"), 1000, 'M')]
    #
    # rdd = spark.sparkContext.parallelize(data)
    # # print(rdd.collect())
    #
    # df = rdd.toDF(["Name","salary","gender"])
    # df.show()



    # Q.25 Change string to date and timestamp

    # list_data = [["2022-03-31 01:55 AM"],
    #              ["2022-03-30 01:15 AM"],
    #              ["2022-03-29 02:15 PM"],
    #              ["2022-04-01 04:15 PM"]]
    #
    # list_schema =["input_col"]
    #
    # df = spark.createDataFrame(list_data,list_schema)
    # df.show()
    # df.printSchema()

    #
    # df1 = df.withColumn("input_col", col("input_col").cast("Date"))
    # df1.printSchema()
    # df1.show()
    #
    # df2 = df1.withColumn("Date", to_date("input_col", "yyyy-MM-dd"))
    # df2.show()
    #
    # df3 = df.withColumn("input_col", col("input_col").cast("string"))
    # df4 = df3.withColumn("Time",to_timestamp("input_col","hh:mm a"))
    # df3.show()


    # Q.26 Get year Month Day Hour Min quater WeekofYear

    # df5 = df4.withColumn("year",year("Time"))\
    #            .withColumn("month",month("Time"))\
    #             .withColumn("day",dayofmonth("Time"))\
    #             .withColumn()


    #Q.27 # Here is dataframe.create two new columns where Full_Name=first_name+last name and if age>50 then AGE_B=Y
    # or else AGE_B=N-->done
    # First_Name,Last_Name,Age
    # Steve,Rogers,85
    # Bruce,Banner,56
    # Tony,Stark,45
    # Stephen,Strage,35
    # Natasha,Romanoff,38


    # expected output:-
    # First_Name,Last_Name,Age,Full_Name,Age_B
    # Steve,Rogers,85, Steve Rogers,Y


    # data=[('Steve','Rogers',85),('Bruce','Banner',56),('Tony','Stark',45),('Stephen','Strage',35),('Natasha','Romanoff',38)]
    # columns=["first_name","last_name","age"]
    #
    # df = spark.createDataFrame(data).toDF(*columns)
    # df.show()

    #way1
    # df1 = df.select("first_name", "last_name","age") \
    #     .withColumn("Fullname", (concat(col("first_name"), lit(" "), col("last_name")))).withColumn("Age_B",when(col("age")>50,'Y').otherwise("null"))
    # df1.show()

    #way2
    # df2 = df.withColumn("Fullname",expr("first_name ||' '|| last_name")).withColumn("Age_B",expr("case when age>50 then 'Y' else 'n' end"))
    # df2.show()

    #way3
    # df3 = df.withColumn("Fullname",concat_ws(' ',"first_name","last_name")).withColumn("Age_B",expr("case when age>50 then 'Y' else 'n' end"))
    # df3.show()

    #Q.28
    # Year, month, sales, modified_date
    # 2020, 1, 1200, 01-12-2020
    # 2020, 1, 1000, 05-12-2020
    # 2020, 1, 1500, 05-12-2020
    # 2020, 2, 800, 01-12-2020
    # 2020, 2, 1200, 05-12-2020
    # 2020, 2, 1000, 01-12-2020
    # Create dataframe out of it and find max sales for each modified date and create result dataframe.

    # df = spark.createDataFrame([(2020, 1, 1200, "01-12-2020"),(2020, 1, 1000, "05-12-2020"),(2020, 1, 1500, "05-12-2020"),(2020, 2, 800, "01-12-2020"),
    #                             (2020, 2, 1200, "05-12-2020"),(2020, 2, 1000, "01-12-2020")],["Year","Month","Sales","Modified_date"])
    # df.show()

    # df1 = df.select("*").select(col("Modified_date").cast("date"))
    # df1.printSchema()

    # w = Window.partitionBy("Modified_date").orderBy(col("Sales").desc())
    # df2 = df.withColumn("Modified_date",to_date(col("Modified_date"),'dd-MM-yyyy')).withColumn("row_number",dense_rank().over(w)).where(col("row_number")==1)
    # df2.show()












