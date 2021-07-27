package sparkcodes.practice

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{Window, WindowSpec}

object PracticeTasks {

  def main(args: Array[String]): Unit= {

    val sparkSession: SparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.builder
        .appName("InClassTasks")
        .master("local[*]")
        .enableHiveSupport()
        .getOrCreate()
    )

    //task 1
    println("---------- TASK 1 -----------")
    val retailDF: DataFrame = sparkSession.read.format("json").load("data/input/RetailData.json")
    retailDF.show(30)
    retailDF.printSchema()

    //task 2
    println("---------- TASK 2 -----------")
    val orderedRetailDF: DataFrame = retailDF.selectExpr(
      "InvoiceNo",
      "InvoiceDateTime",
      "StockCode",
      "Description",
      "cast(UnitPrice as decimal(8,2))",
      "CustomerID",
      "cast(Quantity as int)",
      "Country",
      "Date")
    orderedRetailDF.printSchema()
    println("ordered retail data count: " + orderedRetailDF.count())

    //task 3
    println("---------- TASK 3 -----------")
    val deduplicatedDF: DataFrame = orderedRetailDF.dropDuplicates("InvoiceNo","StockCode","CustomerID","InvoiceDateTime")
    println("Deduplicated Count: " + deduplicatedDF.count())

    //task 4
    println("---------- TASK 4 -----------")
    val withCostPriceCoulmnDF: DataFrame = deduplicatedDF.withColumn("CostPrice",col("Quantity") * col("UnitPrice"))
    withCostPriceCoulmnDF.show(10)

    //task 5
    println("---------- TASK 5 -----------")
    val totalCostPerInvoicePerDayDF: DataFrame = withCostPriceCoulmnDF
      .groupBy("InvoiceNo","Date")
      .agg(sum("CostPrice"))
      .withColumnRenamed("sum(CostPrice)","TotalCost")
    totalCostPerInvoicePerDayDF.show(10)

    //task 6
    println("---------- TASK 6 -----------")
    val maxTotalCost: Row = totalCostPerInvoicePerDayDF.orderBy(desc("TotalCost")).first()
    println("Max total cost: " + maxTotalCost)

    //task 7
    println("---------- TASK 7 -----------")
    val invoiceCustomerCountryDF: DataFrame = withCostPriceCoulmnDF.select("InvoiceNo","CustomerID","Country").distinct()
    invoiceCustomerCountryDF.show(10)

    //task 8
    println("---------- TASK 8 -----------")
    val joinedDF: DataFrame = totalCostPerInvoicePerDayDF
      .join(invoiceCustomerCountryDF,totalCostPerInvoicePerDayDF("InvoiceNo") === invoiceCustomerCountryDF("InvoiceNo"))
      .select("Date","CustomerID","TotalCost")
    //joinedDF.show(10)
    //580729

    //task 9
    println("---------- TASK 9 -----------")
    val filledDF: DataFrame = joinedDF.na.fill("not registered",Seq("CustomerID"))
    filledDF.filter(col("Date") === "2011-12-05").show(10)

    //task 10
    println("---------- TASK 10 -----------")
    val customerTotalCostDF: DataFrame = filledDF
      .filter(col("CustomerID") =!= "not registered")
      .groupBy("CustomerID","Date").agg(sum("TotalCost"))
      .withColumnRenamed("sum(TotalCost)","CustomerTotalCost")
    customerTotalCostDF.show(10)

    //task 11
    println("---------- TASK 11 -----------")
    val orderByWinSpec: WindowSpec = Window.orderBy(desc("CustomerTotalCost"))
    val rankCustomerDF: DataFrame = customerTotalCostDF.withColumn("Rank_Customer", row_number() over orderByWinSpec)
    rankCustomerDF.show(10)

    //task 12
    println("---------- TASK 12 -----------")
    val windowSpec: WindowSpec = Window.partitionBy("Date").orderBy(desc("CustomerTotalCost"))
    val topCustomerDF: DataFrame = rankCustomerDF
      .withColumn("rnum", row_number() over windowSpec)
      .filter(col("rnum") === 1)
      .drop("rnum")
    topCustomerDF.show()

    //task 13
    println("---------- TASK 13 -----------")
    customerTotalCostDF.repartition(4).write.mode("overwrite").format("json").save("data/outputTask13")

    //task 14
    println("---------- TASK 14 -----------")
    customerTotalCostDF.repartition(4).write.mode("overwrite").format("orc").save("data/outputTask14A")
    val dataWithPartitionColumnDF: DataFrame = customerTotalCostDF.withColumn("load_date",lit("2020-09-15"))
    dataWithPartitionColumnDF.repartition(4).write.partitionBy("load_date").mode("overwrite").format("orc").save("data/outputTask14B")
  }

}
