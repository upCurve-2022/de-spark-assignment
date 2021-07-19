/* Spark Assignment Gunpreet Kaur */
package sparkcodes.weekone
/*IMPORTS*/
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max}

object Gunpreetkaur {
  //create a main function and do the rest inside the main function.
  def main(args: Array[String]): Unit = {

    //create a spark session
    val spark=SparkSession
      .builder()
      .master("local")
      .appName("SparkAssignmentGunpreet").getOrCreate()

    /*==========================================TASK1==============================================================*/
    //task 01: Create DataFrame from CSV data. Show your resultant data.
    val dataFrame:DataFrame = spark.read.option("header","true")
      .csv("data/spark_assignment.csv")
    dataFrame.show(false)
    /*==============================================================================================================*/

    /*==========================================TASK2==============================================================*/
    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.
    val dataFrameRename:DataFrame=dataFrame.withColumnRenamed("url","hostname")
    dataFrameRename.show()
    /*==============================================================================================================*/

    /*==========================================TASK3==============================================================*/
    //task 03: Filter "job_title" by any manager. Show your resultant data.
    dataFrame.filter ((col("job_title").contains("Manager")) || (col("job_title").contains("manager"))).show(false)
    /*==============================================================================================================*/

    /*==========================================TASK4==============================================================*/
    //task 04: Highest yearly salary of each gender. Show your resultant data.
    dataFrame.groupBy("gender").agg(max("salary")).show()
    /*==============================================================================================================*/


  }

}

