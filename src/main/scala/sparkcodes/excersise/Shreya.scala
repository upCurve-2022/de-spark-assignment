package sparkcodes.excersise

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}

object Shreya {

  //create a main function and do the rest inside the main function
  def main(args : Array[String]) : Unit = {

    //create a spark session
    val sparkSession : SparkSession = SparkSession.builder()
      .appName("Spark Assignment")
      .master("local[*]")
      .getOrCreate()

    //The dataset file you need to use is spark_assignment.csv

    //task 01: Create DataFrame from CSV data. Show your resultant data.
    val df : DataFrame = sparkSession.read.option("header","true").csv("data/input/spark_assignment.csv")
    df.show(50)

    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.
    val newDF : DataFrame = df.withColumn("hostname", split(col("url"), "/").getItem(2))
    newDF.show(50)

    //task 03: Filter "job_title" by any manager. Show your resultant data.
    df.filter(df("job_title").contains("Manager")).show(50)

    //task 04: Highest yearly salary of each gender. Show your resultant data.
    df.groupBy("gender").agg(max("salary")).show(false)

  }
}
