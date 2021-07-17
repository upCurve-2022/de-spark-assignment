package sparkcodes.weekone

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col}

object PranjalGandhi {
  def main(args:Array[String]):Unit={

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session assignment")
      .getOrCreate()

    //task 01: Create DataFrame from CSV data. Show your resultant data.
    val df = sparkSession.read.option("header","true").
      csv("data/spark_assignment.csv")
    df.show()

    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.
    val dfHostName: DataFrame = df.withColumn("hostnames", functions.split(col("url"),"/").getItem(2))
    dfHostName.show()

    //task 03: Filter "job_title" by any manager. Show your resultant data.
    df.filter(col("job_title").contains("manager")||(col("job_title").contains("Manager"))).show(false)

    //task 04: Highest yearly salary of each gender. Show your resultant data.
    df.groupBy("gender").agg(functions.max("salary")).show()


  }

}
