package sparkcodes.practice

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.time.{Duration, LocalDateTime}

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
    val clickStreamFullDF: DataFrame = spark.read.option("header", "true").format("csv").load("/Users/z0045nq/Documents/IgnitePlus2021/clickstream-data-pipeline/data/input02/clickstream/clickstream_log.csv")
    //clickStreamFullDF.count()
    //clickStreamFullDF.rdd.getNumPartitions

    //Num of partitions 3 - 3 tasks created
    clickStreamFullDF.repartition(3).write.mode("overwrite").format("orc").save("/Users/z0045nq/Documents/IgnitePlus2021/ignite-spark-assignments/data/output")
    val repartClickStreamDF: DataFrame = spark.read.format("orc").load("/Users/z0045nq/Documents/IgnitePlus2021/ignite-spark-assignments/data/output")
    //repartClickStreamDF.count()
    //repartClickStreamDF.rdd.getNumPartitions

    //observe the sql tab - though the filter is performed after group by. In the Plan filter is performed first.
    val sampleInputDF: DataFrame = spark.read.option("header", "true").format("csv").load("/Users/z0045nq/Documents/IgnitePlus2021/ignite-spark-assignments/data/input/spark_assignment.csv")
    val salaryByCountryDF: DataFrame = sampleInputDF.groupBy("country").agg(sum("salary").alias("total_salary"))
    val filterCountryDF: DataFrame  = salaryByCountryDF.filter(col("country") === "China")
    //filterCountryDF.show()

    //observe the sql tab - Here filter is performed after group by unlike above step. Why?
    val filterSalary: DataFrame = salaryByCountryDF.filter(col("total_salary") < 20000)
    //filterSalary.show()

    //null removal with var and loop
    def nullRemovalWithLoop(inputDF: DataFrame, primaryKeyColumns: Seq[String]): DataFrame = {
      var nonNullDF: DataFrame = inputDF
      for (i <- primaryKeyColumns) {
        nonNullDF = inputDF.filter(col(i).isNotNull)
      }
      nonNullDF
    }

    //null removal with map
    def nullRemovalWithMap(inputDF: DataFrame, primaryKeyColumns: Seq[String]): DataFrame = {
      val transformedColumnList: Seq[Column] = primaryKeyColumns.map(x => col(x))
      val nullCheckCondition: Column = transformedColumnList.map(x => x.isNull).reduce(_ || _)
      val inputWithFlagDF: DataFrame = inputDF
        .withColumn(
          "is_primary_key_null",
          when(nullCheckCondition, true).otherwise(false)
        )
      inputWithFlagDF.filter(col("is_primary_key_null") === false)
    }

    //Clickstream data
    val testStartTime01 = LocalDateTime.now()
    nullRemovalWithLoop(
      clickStreamFullDF,
      Seq("session_id", "item_id")
    ).show()
    val testStopTime01 = LocalDateTime.now()
    val timeTaken01: Long = Duration.between(testStartTime01, testStopTime01).toMillis

    val testStartTime02 = LocalDateTime.now()
    nullRemovalWithMap(
      clickStreamFullDF,
      Seq("session_id", "item_id")
    ).show()
    val testStopTime02 = LocalDateTime.now()
    val timeTaken02: Long = Duration.between(testStartTime02, testStopTime02).toMillis

    //item data
    val itemDF: DataFrame = spark.read.option("header", "true").format("csv").load("/Users/z0045nq/Documents/IgnitePlus2021/clickstream-data-pipeline/data/input02/item/item_data.csv")

    val testStartTime03 = LocalDateTime.now()
    nullRemovalWithLoop(
      itemDF,
      Seq("item_id")
    ).count()
    val testStopTime03 = LocalDateTime.now()
    val timeTaken03: Long = Duration.between(testStartTime03, testStopTime03).toMillis

    val testStartTime04 = LocalDateTime.now()
    nullRemovalWithMap(
      itemDF,
      Seq("item_id")
    ).count()
    val testStopTime04 = LocalDateTime.now()
    val timeTaken04: Long = Duration.between(testStartTime04, testStopTime04).toMillis

    //Sample
    val testStartTime05 = LocalDateTime.now()
    nullRemovalWithLoop(
      sampleInputDF,
      Seq("id")
    ).count()
    val testStopTime05 = LocalDateTime.now()
    val timeTaken05: Long = Duration.between(testStartTime05, testStopTime05).toMillis

    val testStartTime06 = LocalDateTime.now()
    nullRemovalWithMap(
      sampleInputDF,
      Seq("id")
    ).count()
    val testStopTime06 = LocalDateTime.now()
    val timeTaken06: Long = Duration.between(testStartTime06, testStopTime06).toMillis

  }
}
