package sparkcodes.weekone
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
object Preethi  {

  //create a main function and do the rest inside the main function.
  def main(args: Array[String]): Unit = {

    //create a spark session
    val sparkSession = SparkSession.builder.master("local").appName("Sparkassignment-Trainng").getOrCreate();

    //task 01: Create DataFrame from CSV data. Show your resultant data.

    val data:DataFrame = sparkSession.read.options(Map("header"->"true")).csv(path ="data/spark_assignment.csv" )
    data.show()

    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.

    val data1 = data.withColumnRenamed("url","hostname").show


   //selecting hostname table
    val dfRename = data.select(col("url").as("hostname")).show


    //task 03: Filter "job_title" by any manager. Show your resultant data.

    val dataFilter = data.filter(data("job_title") === "Manager" ).show()



    //task 04: Highest yearly salary of each gender. Show your resultant data.

     val dfSalary:DataFrame = data.groupBy("gender").agg(functions.max("salary"))
    dfSalary.show()


  }
}
