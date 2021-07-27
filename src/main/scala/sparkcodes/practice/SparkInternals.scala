package sparkcodes.practice

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SparkInternals {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("InClassTasks")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    //spark-shell - by default allocates 1 executor and all the cores in your system
    //spark-shell --num-executors 1 --executor-cores 2 (--executor-memory, --driver-memory)
    //Num of partitions 1 - one task created
    val clickStreamFullDF: DataFrame = spark.read.option("header", "true").format("csv").load("/Users/z0045nq/Documents/IgnitePlus2021/clickstream-data-pipeline/data/input02/clickstream/view_log.csv")
    clickStreamFullDF.count()
    clickStreamFullDF.rdd.getNumPartitions

    //Num of partitions 3 - 3 tasks created
    clickStreamFullDF.repartition(3).write.mode("overwrite").format("orc").save("/Users/z0045nq/Documents/IgnitePlus2021/ignite-spark-assignments/data/output")
    val repartClickStreamDF: DataFrame = spark.read.format("orc").load("/Users/z0045nq/Documents/IgnitePlus2021/ignite-spark-assignments/data/output")
    repartClickStreamDF.count()
    repartClickStreamDF.rdd.getNumPartitions

    //observe the sql tab - though the filter is performed after group by. In the Plan filter is performed first.
    val sampleInputDF: DataFrame = spark.read.option("header", "true").format("csv").load("/Users/z0045nq/Documents/IgnitePlus2021/ignite-spark-assignments/data/input/spark_assignment.csv")
    val salaryByCountryDF: DataFrame = sampleInputDF.groupBy("country").agg(sum("salary").alias("total_salary"))
    val filterCountryDF: DataFrame  = salaryByCountryDF.filter(col("country") === "China")
    filterCountryDF.show()

    //observe the sql tab - Here filter is performed after group by unlike above step. Why?
    val filterSalary: DataFrame = salaryByCountryDF.filter(col("total_salary") < 20000)
    filterSalary.show()
  }

}
