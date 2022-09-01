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

    custm_df = spark.read.csv(r"D:\Apache Spark Data\spark_files\garage_all_tables\customer.csv",inferSchema=True,header=True)
    #custm_df.show()

    vendor_df = spark.read.csv(r"D:\Apache Spark Data\spark_files\garage_all_tables\vendors.csv",inferSchema=True,header=True)
    #vendor_df.show()

    emp_df = spark.read.csv(r"D:\Apache Spark Data\spark_files\garage_all_tables\employee.csv",inferSchema=True,header=True)
    #emp_df.show()

    sparepart_df = spark.read.csv(r"D:\Apache Spark Data\spark_files\garage_all_tables\sparepart.csv",inferSchema=True,header=True)
    #sparepart_df.show()

    purchase_df = spark.read.csv(r"D:\Apache Spark Data\spark_files\garage_all_tables\purchase.csv",inferSchema=True,header=True)
    #purchase_df.show()

    ser_det_df = spark.read.csv(r"D:\Apache Spark Data\spark_files\garage_all_tables\ser_det.csv",inferSchema=True,header=True)
    #ser_det_df.show()


    # Q.1 List all the customers serviced.

    df = custm_df.join(ser_det_df, on=custm_df.CID == ser_det_df.CID,
                                how='inner').select([custm_df.CID,custm_df.CNAME,ser_det_df.CID])
    #df.show()


    # Q.2 Customers who are not serviced.

    df1 = custm_df.join(ser_det_df, on=custm_df.CID == ser_det_df.CID,
                       how='left_anti')
    #df1.show()


    # Q.3 Employees who have not received the commission.

    df2 = emp_df.join(ser_det_df, on=emp_df.EID == ser_det_df.EID,
                       how='inner').select([emp_df.EID,emp_df.ENAME,ser_det_df.EID,ser_det_df.COMM]).where(ser_det_df.COMM == 0)
    #df2.show()

    # Q.4 Name the employee who have maximum Commission

    # windowspec = Window.partitionBy().orderBy(col("COMM").desc())

    # df3 = emp_df.join(ser_det_df, on=emp_df.EID == ser_det_df.EID,
    #                   how='inner').select([emp_df.ENAME, ser_det_df.EID, ser_det_df.COMM])\
    #                             .withColumn("max_comm",dense_rank().over(windowspec)).filter("max_comm = 1")
    #
    # df3.show()

    # Q.5 Show employee name and minimum commission amount received by an employee.

    # windowspec = Window.partitionBy().orderBy(col("COMM").asc())
    #
    # df4 = emp_df.join(ser_det_df, on=emp_df.EID == ser_det_df.EID,
    #                   how='inner').select([emp_df.ENAME, ser_det_df.COMM]) \
    #                             .withColumn("min_comm", dense_rank().over(windowspec)).filter("min_comm = 1")
    # df4.show()



    # Q.6 Display the Middle record from any table.-------------ERROR

    # df5 = emp_df.select("*").withColumn("rno",row_number())\
    #                         #.filter("rno in (select round(count(*)/2")
    # df5.show()


    # Q.7 Display last 4 records of any table.

    # df6 = emp_df.select("EId","ENAME")
    # print(df6.tail(4))

    # Q.8 Count the number of records without count function from any table.

    #print(emp_df.count())


    # Q.9 Delete duplicate records from "Ser_det" table on cid.(note Please rollback after execution).

    df7 = custm_df.join(ser_det_df, on=custm_df.CID == ser_det_df.CID,
                       how='inner').select([custm_df.CID, custm_df.CNAME, ser_det_df.CID]).drop_duplicates()
    #df7.show()

    # Q.10 Show the name of Customer who have paid maximum amount

    # windowspec = Window.partitionBy().orderBy(col("TOTAL").desc())
    #
    # df8 = custm_df.join(ser_det_df, on=custm_df.CID == ser_det_df.CID,
    #                     how='inner').select([custm_df.CID, custm_df.CNAME, ser_det_df.TOTAL])\
    #                             .withColumn("total_amount",dense_rank().over(windowspec)).filter("total_amount = 1 ")
    # df8.show()

    # Q.11 Display Employees who are not currently working.-----ERROR

    # inputdf = emp_df.select(col("EDOL").cast("date"))
    # inputdf.printSchema()
    #
    # df9 = emp_df.filter("EDOL < sysdate")
    # df9.show()

    # Q.12 How many customers serviced their two wheelers.

    df10 = custm_df.join(ser_det_df, on=custm_df.CID == ser_det_df.CID,
                        how='inner').select([custm_df.CID, custm_df.CNAME, ser_det_df.TYP_VEH])\
                                    .filter(ser_det_df.TYP_VEH == 'TWO WHEELER')
    #df10.show()

    # Q.13 List the Purchased Items which are used for Customer Service with Unit of that Item.

    df11 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner')\
                                    .join(sparepart_df,ser_det_df.SPID==sparepart_df.SPID, how='inner')\
                                    .select([custm_df.CNAME,sparepart_df.SPNAME,sparepart_df.SPUNIT,ser_det_df.QTY])
    #df11.show()

    # Q.14 Customers who have Colored their vehicles.

    df12 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID,how='inner')\
                                    .select([custm_df.CNAME,ser_det_df.TYP_SER]).filter(ser_det_df.TYP_SER == 'COLOR')
    #df12.show()


    # Q.15 Find the annual income of each employee inclusive of Commission

    df13 = ((emp_df.join(ser_det_df, on=emp_df.EID==ser_det_df.EID, how='left')\
             .select([emp_df.EID,emp_df.ENAME,emp_df.ESAL,ser_det_df.COMM])).groupBy("EID")\
                                .agg(sum("COMM").alias("COMM"))).join(emp_df,emp_df.EID==emp_df.EID,"inner").\
                    fillna(0,subset=["COMM","ESAL"]).select("*",expr("(ESAL+COMM) as totoa1"))

    #df13.show()


    # Q.16 Vendor Names who provides the engine oil.

    df14 = vendor_df.join(purchase_df, on=vendor_df.VID==purchase_df.VID, how='inner')\
                                            .join(sparepart_df, on=purchase_df.SPID==sparepart_df.SPID)\
                                .select([vendor_df.VNAME,sparepart_df.SPNAME]).\
                                    filter(col("SPNAME").like("%ENGINE OIL%"))
    #df14.show()


    # Q.17 Total Cost to purchase the Color and name the color purchased.

    df16 = sparepart_df.join(purchase_df, on=sparepart_df.SPID == purchase_df.SPID, how='left') \
        .select([sparepart_df.SPNAME, purchase_df.TOTAL]).groupBy("SPNAME").agg(sum("TOTAL").alias("total"))\
        .filter(col("SPNAME").like("%COLOR%"))
    # df16.show()


    # Q.18 Purchased Items which are not used in "Ser_det".

    df17 = sparepart_df.join(purchase_df, on=sparepart_df.SPID==purchase_df.SPID, how='inner')\
                                .join(ser_det_df,on=purchase_df.SPID==ser_det_df.SPID, how='left_anti')\
                                .select([sparepart_df.SPID,sparepart_df.SPNAME])
    #df17.show()


    # Q.19 Spare Parts Not Purchased but existing in Sparepart

    df18 = sparepart_df.join(purchase_df, on=sparepart_df.SPID == purchase_df.SPID, how='left_anti')
    #df18.show()


    # Q.21 Specify the names of customers who have serviced their vehicles more than one time.

    df19 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner')\
                                    .select([custm_df.CNAME,custm_df.CID,ser_det_df.CID])\
                                    .groupBy("CNAME").agg(count(ser_det_df.CID).alias("cnt")).where("cnt > 1")
    #df19.show()


    # Q.22 List the Items purchased from vendors locationwise.

    df20 = sparepart_df.join(purchase_df, on=sparepart_df.SPID==purchase_df.SPID, how='inner')\
                        .join(vendor_df, on=purchase_df.VID==vendor_df.VID).select([sparepart_df.SPNAME,vendor_df.VADD])
    #df20.show()


    # Q.24 Display name of customers who paid highest SPGST and for which item

    df21 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner')\
                .join(sparepart_df, on=ser_det_df.SPID==sparepart_df.SPID, how='inner')\
                                    .join(purchase_df, on=sparepart_df.SPID==purchase_df.SPID)\
                            .select([custm_df.CID,custm_df.CNAME,sparepart_df.SPNAME,purchase_df.SPGST])
    #df21.show()

    # Q.26  list name of item and employee name who have received item

    df22 = emp_df.join(purchase_df, on=emp_df.EID==purchase_df.RCV_EID, how='inner')\
                    .join(sparepart_df, on=purchase_df.SPID==sparepart_df.SPID, how='inner')\
                    .select([purchase_df.SPID,emp_df.ENAME,sparepart_df.SPNAME])
    #df22.show()

    #  Q.27 Display the Name and Vehicle Number of Customer who serviced his vehicle, And Name the Item used for
    #  Service, And specify the purchase date of that Item with his vendor and Item Unit and Location, And employee
    #  Name who serviced the vehicle. for Vehicle NUMBER "MH-14PA335".'

    df23 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner')\
                                .join(sparepart_df,on=ser_det_df.SPID==sparepart_df.SPID, how='inner')\
                                        .join(purchase_df,on=sparepart_df.SPID==purchase_df.SPID, how='inner')\
                                            .join(vendor_df, on=purchase_df.VID==vendor_df.VID, how='inner')\
                                                .join(emp_df, on=ser_det_df.EID==emp_df.EID, how='inner')\
        .select([custm_df.CNAME,ser_det_df.VEH_NO,sparepart_df.SPNAME,purchase_df.PDATE,vendor_df.VNAME,sparepart_df.SPUNIT,
                 vendor_df.VADD,emp_df.ENAME]).where("VEH_NO == 'MH-14PA335'")
    #df23.show()


    # Q.28 who belong this vehicle  MH-14PA335" Display the customer name

    df24 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner').select([custm_df.CNAME,ser_det_df.VEH_NO])\
                            .filter("VEH_NO == 'MH-14PA335'")
    #df24.show()


    # Q.29 Display the name of customer who belongs to New York and when he /she service their  vehicle on which date

    df25 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner').select([custm_df.CNAME,custm_df.CADD,
                                ser_det_df.VEH_NO,ser_det_df.SER_DATE]).where("CADD == 'NEW YORK'")
    #df25.show()


    # Q 30 from whom we have purchased items having maximum cost?

    # windowSpace = Window.partitionBy().orderBy(col("TOTAL").desc())
    #
    # df26 = vendor_df.join(purchase_df, on=vendor_df.VID==purchase_df.VID, how='inner')\
    #             .join(sparepart_df, on=purchase_df.SPID==sparepart_df.SPID, how='inner').select([vendor_df.VNAME,
    #                     sparepart_df.SPNAME,purchase_df.TOTAL]).withColumn("max_cost",dense_rank().over(windowSpace))\
    #                                     .filter("max_cost = 1")
    # df26.show()

    # Q.31 Display the names of employees who are not working as Mechanic and that employee done services

    df27 = emp_df.join(ser_det_df, on=emp_df.EID==ser_det_df.EID, how='inner').select([emp_df.EID,emp_df.ENAME,
                                                                                       emp_df.EJOB])\
                                    .filter("EJOB != 'MACHANIC'")
    #df27.show()

    # Q32 Display the various jobs along with total number of employees in each job. The output should
    # contain only those jobs with more than two employees.----ERROR

    df28 = emp_df.select("EJOB").groupBy("EJOB").agg((count("*")>2).alias("Count"))
    #df28.show()

    # Q33 Display the details of employees who done service  and give them rank according to their no. of services .

    # Q 34 Display those employees who are working as Painter and fitter and who provide service and
    # total count of service and painter done by fitter

    df111 = emp_df.join(ser_det_df, on=emp_df.EID==ser_det_df.EID, how='inner').select([ser_det_df.EID,emp_df.ENAME,emp_df.EJOB])\
                .groupBy(ser_det_df.EID,emp_df.ENAME,emp_df.EJOB).agg(count("EID").alias("cnt"))\
                    .where("EJOB = 'PAINTER' or EJOB = 'FITTER'")
    #df111.show()

    # Q.34.Display employee salary and as per highest  salary provide Grade to employee.

    df29 = emp_df.select("EID","ENAME","ESAL").withColumn("Grade",when(col("ESAL").between(0, 1200),"D")\
            .otherwise(when(col("ESAL").between(1201,2400),"C").otherwise(when(col("ESAL").between(2401,3600),"B")\
            .otherwise(when(col("ESAL").between(3601,4800),"A").otherwise("E")))))
    #df29.show()


    # Q.36 display the 4th record of emp table without using group by and rowid

    # windowspace = Window.partitionBy().orderBy(asc("EID"))
    # df30 = emp_df.select("*").withColumn("Fourth_record",row_number().over(windowspace))\
    #                                 .filter("Fourth_record == 4")
    # df30.show()

    # Q37 Provide a commission 100 to employees who are not earning any commission.

    df31 = emp_df.join(ser_det_df, on=emp_df.EID==ser_det_df.EID, how='inner').select(emp_df.EID,emp_df.ENAME,ser_det_df.COMM)\
                            .withColumn("Earn_comm",when(col("COMM").isin(0),100).otherwise(col("COMM")))
    #df31.show()

    # Q38 write a query that totals no. of services  for each day and place the results
    # in descending order

    df32 = ser_det_df.select("SER_DATE").groupBy("SER_DATE").count().orderBy(col("SER_DATE").desc())
    #df32.show()


    # Q39 Display the service details of those customer who belong from same city------doubt

    df33 = ser_det_df.join(custm_df, on=ser_det_df.CID==custm_df.CID, how='inner').select([custm_df.CID,custm_df.CADD])\
                            .groupBy("CID","CADD").agg(count(col("CADD")).alias("Cnt"))
    #df33.show()


    # Q.40 write a query join customers table to itself to find all pairs of------------ERROR
    # customers service by a single employee

    # df34 = custm_df.alias("df1").join(custm_df.alias("df2"),on=df1.CID1==df2.CID2, how='self')\
    #                             .select(["CID1","CID2"])
    #df34.show()


    # Q41 List each service number follow by name of the customer who
    # made  that service

    df35 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner').select([ser_det_df.SID,custm_df.CNAME])
    #df35.show()


    # Q42 Write a query to get details of employee and provide rating on basis of  maximum services provide by employee
    # .Note (rating should be like A,B,C,D)

    # df36 = emp_df.join(ser_det_df, on=emp_df.EID==ser_det_df.EID, how='inner').select([emp_df.EID,emp_df.ENAME,ser_det_df.EID]) \
    #     .withColumn("Grade",when(col("EID").isin(3004),"A").otherwise(when(col("EID").isin(3002),"B")\
    #         .otherwise(when(col("EID").isin(3003),"C").otherwise(when(col("EID").isin(3001),"D").otherwise("null")))))
    #df36.show()


    # Q43 Get the details of customers with his total no of services ?

    df37 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner')\
        .select([custm_df.CID,custm_df.CNAME,custm_df.CADD]).groupBy(custm_df.CID,custm_df.CNAME,custm_df.CADD)\
        .agg(count("CID").alias("No_Of_Services")).orderBy(col("No_Of_Services").desc())
    #df37.show()


    # Q44 Write a query to get maximum service amount of each customer with their customer details ?


    df38 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner').select([custm_df.CID,custm_df.CNAME,
                                custm_df.CADD,ser_det_df.SER_AMT]).groupBy(custm_df.CID,custm_df.CNAME,custm_df.CADD)\
                                .agg(max("SER_AMT").alias("Max_amt"))
    #df38.show()


    # Q45 From which location sparpart purchased  with highest cost ?

    windowspace = Window.partitionBy().orderBy(col("TOTAL").desc())

    df39 = vendor_df.join(purchase_df, on=vendor_df.VID==purchase_df.VID, how='inner').select([vendor_df.VADD,purchase_df.TOTAL])\
                .withColumn("Highest_Cost", dense_rank().over(windowspace)).filter("Highest_Cost == 1")
    #df39.show()


    # Q46 Get the details of employee with their service details who has salary is null

    df40 = emp_df.join(ser_det_df, on=emp_df.EID==ser_det_df.EID, how='inner').select([emp_df.EID,emp_df.ENAME,emp_df.ESAL])\
                        .filter("ESAL is null")
    #df40.show()


    # Q.47 find the sum of purchase location wise

    df41 = vendor_df.join(purchase_df, on=vendor_df.VID==purchase_df.VID, how='inner').select([vendor_df.VADD,purchase_df.TOTAL])\
                    .groupBy(vendor_df.VADD).agg(sum("TOTAL").alias("purchase_amt"))
    #df41.show()


    # Q.48 write a query sum of purchase amount in word location wise ?


    # Q.49 Has the customer who has spent the largest amount money has been give highest rating-----ERROR

    windowspace = Window.partitionBy().orderBy(col("TOTAL").desc())

    df43 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner').select([custm_df.CNAME,ser_det_df.TOTAL])\
            .withColumn("Rating", dense_rank().over(windowspace))
    # df43.show()


    # Q50 select the total amount in service for each customer for which the total is greater than the amount of
    # the largest service amount in the table


    #  Q.51  List the customer name and sparepart name used for their vehicle and  vehicle type

    df45 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner')\
            .join(sparepart_df, on=ser_det_df.SPID==sparepart_df.SPID, how='inner').select([custm_df.CNAME,sparepart_df.SPNAME,\
            ser_det_df.TYP_VEH,ser_det_df.VEH_NO])
    #df45.show()


    # Q.52 Write a query to get spname ,ename,cname quantity ,rate ,service amount for record exist in service table

    df46 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner').join(sparepart_df,\
                on=ser_det_df.SPID==sparepart_df.SPID, how='inner').join(emp_df, on=ser_det_df.EID==emp_df.EID)\
                .select([sparepart_df.SPNAME,emp_df.ENAME,custm_df.CNAME,ser_det_df.QTY,ser_det_df.SP_RATE,ser_det_df.SER_AMT])
    #df46.show()


    # Q.53 specify the vehicles owners who’s tube damaged.

    df47 = custm_df.join(ser_det_df, on=custm_df.CID==ser_det_df.CID, how='inner').select(custm_df.CID,custm_df.CNAME,\
                    ser_det_df.TYP_SER).filter("TYP_SER = 'TUBE DAMAGED'")
    #df47.show()


    # Q.54 Specify the details who have taken full service.

    df48 = custm_df.join(ser_det_df, on=custm_df.CID == ser_det_df.CID, how='inner').select(custm_df.CID,custm_df.CNAME,\
                                                                                            ser_det_df.TYP_SER).filter(
        "TYP_SER = 'FULL SERVICING'")
    #df48.show()


    # Q.55 Select the employees who have not worked yet and left the job.


    # Q.56  Select employee who have worked first ever.

    df50 = emp_df.join(ser_det_df, on=emp_df.EID==ser_det_df.EID, how='inner').select([emp_df.EID,emp_df.ENAME,emp_df.EDOJ,
                    emp_df.EDOL,ser_det_df.SER_DATE]).agg(min("SER_DATE").alias("MIN_SER_DATE"))
    #df50.show()


    # Q.57 Display all records falling in odd date

    # Q.58 Display all records falling in even date

    # Q.59 Display the vendors whose material is not yet used.

    df51 = vendor_df.join(purchase_df, on=vendor_df.VID==purchase_df.VID, how='left_anti')\
        .join(ser_det_df, on=purchase_df.SPID==ser_det_df.SPID, how='inner').select([vendor_df.VID,vendor_df.VNAME])
    #df51.show()


    # Q.60 Difference between purchase date and used date of spare part.






