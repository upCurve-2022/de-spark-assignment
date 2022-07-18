package sparkcodes.excersise

import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.sql.functions._

object SampleTemplate {

  //create a main function and do the rest inside the main function.
  def main( args: Array[String]) : Unit = {

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
    val df = sparkSession.read.option("header",true).csv("data/input/spark_assignment.csv")
    df.printSchema()
    df.show(10)

    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.
    val findHostname: (String=>String) = (arg: String) => {
      val temp = arg.split( "\\/\\/")(1).split( "\\/")
      temp(0)
    }

    val sqlfunc = udf(findHostname)
    val ndf =  df.withColumn( colName = "hostname", sqlfunc(df.col( "url")))
    ndf.show(2)

    //task 03: Filter "job_title" by any manager. Show your resultant data.
    ndf.filter(ndf("job_title").contains("Manager")).show()

    //task 04: Highest yearly salary of each gender. Show your resultant data.
    ndf.createOrReplaceTempView ("view1")
    val a = sparkSession.sql("SELECT MAX(Salary) AS maxSalary,Gender FROM view1 GROUP BY Gender")
    a.show()
  }
}
