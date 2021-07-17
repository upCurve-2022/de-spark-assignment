//Meghana Dinesh
//Spark Assignment


package sparkcodes.weekone
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

object Meghana {
  def main(args: Array[String]): Unit = {

    //create a spark session

    val spark=SparkSession
      .builder()
      .master("local")
      .appName("Working with Databases").getOrCreate()

    //task 01: Create DataFrame from CSV data. Show your resultant data.

    val df = spark.read.options(Map("header"->"true"))
      .csv("E:\\spark-assignments-master\\data\\spark_assignment.csv")
    df.show()

    //###############################################################################################################


    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.

    val df1=df.withColumnRenamed("url","hostname")
    df1.show()//Displays the entire data with url renamed to hostname
        df1.select(col("hostname")).show()//Displays only the column when renamed to hostname

    //###############################################################################################################


    //task 03: Filter "job_title" by any manager. Show your resultant data.
    df.filter ((col("job_title").contains("Manager")) || (col("job_title").contains("manager"))).show(false)

  //#################################################################################################################

    ////task 04: Highest yearly salary of each gender. Show your resultant data.
    df.groupBy("gender").agg(max("salary")).show()
    // ###############################################################################################################

  }

}
