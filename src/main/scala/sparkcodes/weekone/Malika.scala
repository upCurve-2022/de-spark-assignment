import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Malika {


  //create a main function and do the rest inside the main function.
  def main(args: Array[String]): Unit = {

    //create a spark session
    val spark = SparkSession.builder().master("local").getOrCreate()

    //task 01: Create DataFrame from CSV data. Show your resultant data.
    val df: DataFrame = spark.read
      .option("header", "true")
      .csv("data_assignment/spark_assignment.csv")
    df.show()

    //task 02: Create a new column "hostname" derived from "url" column. Show your resultant data.
    val hostNameDf:DataFrame= df.withColumn("Hostname",functions.split(col("url"),":").getItem(0) )
    hostNameDf.show()

    //task 03: Filter "job_title" by any manager. Show your resultant data.
    val Job=df.filter(col("job_title")==="Human Resources Manager")
    Job.show()

    //task 04: Highest yearly salary of each gender. Show your resultant data.
    val Salary:DataFrame = df.groupBy("gender").agg(functions.max("salary"))
    Salary.show()

  }

}
