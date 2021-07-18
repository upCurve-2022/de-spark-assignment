import io.netty.handler.codec.smtp.SmtpRequests.data
import org.apache.spark
import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.reflect.internal.util.NoSourceFile.path
object shivaniSen {

  //create a main function and do the rest inside the main function.
  def main(args: Array[String]): Unit = {

    //create a spark session
    val sparkOne = SparkSession.builder.master("local").getOrCreate()

    //task 01: Create DataFrame from CSV data. Show your resultant data.
    val dataOne : DataFrame = sparkOne.read.option("header","true").csv( path="data/spark_assignment.csv")
    dataOne.show(false)

    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.
    val dataOneHost:DataFrame= dataOne.withColumn("hostname",split(col("url"),":").getItem(0) )
    dataOneHost.show()

    //task 03: Filter "job_title" by any manager. Show your resultant data.
    dataOne.filter((col("job_title").contains("Manager"))).show(false)


    //task 04: Highest yearly salary of each gender. Show your resultant data.
    dataOne.groupBy("gender").agg(functions.max("salary")).show(false)

  }
}
