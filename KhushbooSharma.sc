package sparkcodes.excersise

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object khushboo_sharma {
  def main(args: Array[String]) {


    val sparkSession:SparkSession=SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("khushboo_sharma")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )
    //task 01: Create DataFrame from CSV data. Show your resultant data.

    println("-------------Task1----------------------")
    //val df:DataFrame = sparkSession.read.format("csv").load("data/input/spark_assignment.csv")
    val df=sparkSession.read.option("header","true").csv("data/input/spark_assignment.csv")
    df.show(30)
    df.printSchema()


    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.

    println("-------------Task2----------------------")


    val splitData:(String=>String)= (arg:String)=>{
      val stringSplitter=arg.split("\\/\\/")(1).split("\\/")
      stringSplitter(0)
    }
    val sqlFun=udf(splitData)
    val HostDf:DataFrame=df.withColumn("hostname",sqlFun(df.col("url")))
    HostDf.show(30)
    //task 03: Filter "job_title" by any manager. Show your resultant data.

    println("-------------Task3----------------------")
    val job:DataFrame=df.filter(col("job_title").like("%Manager%"))
    job.show()
    //task 04: Highest yearly salary of each gender. Show your resultant data.
    println("-------------Task4----------------------")
    val g_by:DataFrame=df.groupBy("gender").agg(max("salary"))
    g_by.show(30)
  }


}
