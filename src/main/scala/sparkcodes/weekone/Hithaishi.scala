package sparkcodes.weekone
import org.apache.spark.sql.functions.{col, length, lit, map_values, regexp_replace, split, when}
import org.apache.spark.sql.types.{FloatType, IntegerType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, RelationalGroupedDataset, Row, SparkSession}

object Hithaishi {
  def main(args: Array[String]): Unit = {
    val sparkSession:SparkSession=SparkSession.builder.master("local[1]").appName("Sparkassignment-Training").getOrCreate

// ******************************************  Task 1  *****************************************************************
// Create DataFrame from CSV data. Show your resultant data.

    val data:DataFrame= sparkSession.read.option("header","true").csv("data/spark_assignment.csv")
    data.show(false)


// ******************************************  Task 2  *****************************************************************
// Create a new column "hostname" derived from "url" column. Show your resultant data.

    val hostDf:DataFrame= data.withColumn("hostname",split(col("url"),":").getItem(0) )
    hostDf.show()


// *********************************************  Task 3  **************************************************************
// Filter "job_title" by any manager. Show your resultant data.

    val jobDf=data.filter(col("job_title")==="Human Resources Manager")
    jobDf.show(false)


// ***********************************************  Task 4  ************************************************************
// Highest yearly salary of each gender. Show your resultant data.

    // To remove "$" from the salary
    val salDf=data.withColumn("new salary",regexp_replace(col("salary"),"\\$",""))
    salDf.show()

    //To convert salary from string to float so that max operation can be performed
    val salDf2=salDf.withColumn("new salary", col("new salary").cast(FloatType))
    salDf2.show()

    // Find highest salary based on gender
    val maxSalary = salDf2.groupBy("gender").max("new salary")
    maxSalary.show()


  }

}
