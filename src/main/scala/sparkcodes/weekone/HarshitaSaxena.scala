package sparkcodes.weekone
import org.apache.spark
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object HarshitaSaxena {

  //create a main function and do the rest inside the main function.
  def main(args: Array[String]): Unit = {

    //create a spark session
    val spark = SparkSession.builder.master("local").appName("SparkAssignment").getOrCreate()


    //task 01: Create DataFrame from CSV data. Show your resultant data.
    val dframe:DataFrame = spark.read.options(Map("header"->"true")).csv("C:/Users/harsh/IdeaProjects/spark-assignments/data/spark_assignment.csv")
    dframe.show()

    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.
    val dfCreateNewColumns: DataFrame = dframe.withColumn("hostnames", functions.split(col("url"),"/").getItem(2))
    dfCreateNewColumns.show()


    //task 03: Filter "job_title" by any manager. Show your resultant data.
    dfCreateNewColumns.filter(dframe("job_title") === "Project Manager").show(false)


    //task 04: Highest yearly salary of each gender. Show your resultant data.
   dframe.groupBy("gender").agg(functions.max("salary")).show(false)

  }}
