from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split,col,row_number,when
from pyspark.sql.window import Window


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Spark_Practice").getOrCreate()

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

    # df = spark.createDataFrame([(1,"a/b"),(2,"a"),(3,"a/b/c")],["num1","num2"])
    # df.show()
    #
    # df1 = df.withColumn("new_col",explode(split(col("num2"),"/"))).drop("num2")
    # df1.show()
    #
    # df2 = df.select("*",explode(split(col("num2"),"/"))).drop("num2")
    # df2.show()



    # Q.5  what will be inner join,left outer join,right outer join
    #  tab1 tab2
    #  1     2
    #  2     1
    #  2     2
    #  1     1

    # df = spark.createDataFrame([(1,),(2,),(2,),(1,)],["tab1"])
    # df.show()

    # df1 = spark.createDataFrame([(2,),(1,),(2,),(1,)],["tab2"])
    # df1.show()
    #
    # df3 = df.join(df1,df.tab1==df1.tab2,'inner')
    # df3.show()
    #
    # df4 = df.join(df1, df.tab1 == df1.tab2, 'left')
    # df4.show()
    #
    # df5 = df.join(df1, df.tab1 == df1.tab2, 'right')
    # df5.show()
    #
    # df6 = df.join(df1, df.tab1 == df1.tab2, 'left_anti')
    # df6.show()

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

    # df2 = spark.createDataFrame([(1,),(2,),(3,),(4,),(5,),(6,),(7,)],["col1"])
    # df2.show()
    #
    # df3 = spark.createDataFrame([("a",),("b",),("c",),("d",)],["col2"])
    # df3.show()
    #
    # w = Window.partitionBy().orderBy("col1")
    # w1 = Window.partitionBy().orderBy("col2")
    #
    # df4 = df2.withColumn("new_col1",row_number().over(w))
    # df4.show()

    # df5 = df3.withColumn("new_col2",row_number().over(w1))
    # df5.show()
    #
    # df6 = df4.join(df5,df4.new_col1==df5.new_col2,'outer').drop("col1").drop("col2")
    # df6.show()




    # input=  Gender        output=  Gender  Category
    #          Male                   Male      M
    #         Female                 Female     F

    # df1 = spark.createDataFrame([("Male",),("Female",)],["Gender"])
    # df1.show()
    #
    # df2 = df1.withColumn("Gender",when(col("Gender")=="Male","M").otherwise("F"))
    # df2.show()




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
    #
    # df = spark.sparkContext.parallelize([(1,"a",2),(2,"b",3),(3,"c","null")]).toDF(["id","name","mgrid"])
    # df.show()
    #
    #
    # Data = [(1,"a",2),(2,"b",3),(3,"c",'')]
    # col = ["id","name","mgrid"]
    # df1 = spark.createDataFrame("Data").toDF(*col)
    # df1.show()




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

    df5 = spark.createDataFrame([(1,),(1,),(1,),(1,),(0,),(0,),(0,)],["input"])
    df5.show()

    w = Window.partitionBy("input").orderBy("input")
    df6 = df5.withColumn("output",row_number().over(w))
    df6.orderBy("output").show()


