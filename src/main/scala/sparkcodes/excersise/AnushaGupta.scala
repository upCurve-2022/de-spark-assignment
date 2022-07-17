package sparkcodes.excersise
import org.apache.spark
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object AnushaGupta {
  //create a main function and do the rest inside the main function.
  def main(args: Array[String]) : Unit = {
    //create a spark session
    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("InClassTasks")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    //The dataset file you need to use is spark_assignment.csv
    //task 01: Create DataFrame from CSV data. Show your resultant data.
    val dataFrame = sparkSession.read.option("header", "true").csv("/Users/Z00B3CZ/Downloads/de-spark-assignment/data/input/spark_assignment.csv")
    dataFrame.show()

    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.
    val splitColData : (String=>String)=(arg:String)=>{
      val stringSplitarr = arg.split("\\/\\/")(1).split("\\/")
      stringSplitarr(0)
    }
    val sqlfunc = udf(splitColData)
    val newDataFrame = dataFrame.withColumn("hostname", sqlfunc(dataFrame.col("url")))
    newDataFrame.show()

    //task 03: Filter "job_title" by any manager. Show your resultant data.
    newDataFrame.filter(newDataFrame("job_title").contains("Manager")).show()

    //task 04: Highest yearly salary of each gender. Show your resultant data.
    newDataFrame.createOrReplaceTempView("peopleData")
    val sqlDf = sparkSession.sql("SELECT MAX(Salary) AS max_salary,Gender FROM peopleData GROUP BY Gender")
    sqlDf.show()
  }

}
