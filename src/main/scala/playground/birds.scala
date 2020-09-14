package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.UnboundedPreceding
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.util.random

import org.apache.log4j.Logger
import org.apache.log4j.Level


object birds extends App {

  val spark = SparkSession.builder().appName("birds").
    config("spark.master","local").
    getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  val input_schema = new StructType(
    Array(
      StructField("Species",StringType),
      StructField("Category",StringType),
      StructField("Period",StringType),
      StructField("Annual percentage change",DoubleType)
    )
  )


    val df1 = spark.read.format("csv")
    .schema(input_schema)
    .option("header","True")
    .load("/Users/debasmitadas/Documents/spark-essentials-master/src/main/resources/data/Birds")

  df1.printSchema()

  val upper: String => String = _.toUpperCase
  val upperUDF=udf(upper)

  def species  = (str1: String) =>  {
  val str2 = str1.split(" ")(0)
    str2
  }
  val speciesUDF = spark.udf.register("speciesUDF",species)

  def apcfunc = (apc1: Double)=>{
    var x = "increasing"
    if (apc1 > 2) {
      x = "increasing"
    }
    else {
      x = "decreasing"
    }
    x
  }
  val apcfuncUDF = spark.udf.register("apcfuncUDF",apcfunc)

  df1.printSchema()
  df1.show(2)

  df1.withColumn("Species Name",speciesUDF(col("species"))).
    withColumn(colName = "APC Trend",apcfuncUDF(col("Annual percentage change"))).
    show(4)

  df1.withColumn("Species Name",split(col("Species")," ")(0)).
    withColumn("APC Trend",when(col("Annual percentage change") > 2, "increasing" ).
      otherwise("decreasing")).
  withColumn("Period start Date",substring(col("Period"),2,4)).
    show(4)

  val cum = Window.partitionBy("Category").
    rowsBetween(Window.unboundedPreceding,Window.currentRow)


  df1.withColumn("Cumulative Sum",sum("Annual percentage change").over(cum))
    .show(40)

  df1.groupBy("Category").
    agg(sum("Annual percentage change").alias("Sum")).
    show(20)



  // Spark UDF Example
//  def cleanCountry = (country: String) => {
//    val allUSA = Seq("US", "USa", "USA", "United states", "United states of America")
//    if (allUSA.contains(country)) {
//      "USA"
//    }
//    else {
//      "unknown"
//    }
//  }
//  val normaliseCountry = spark.udf.register("normalisedCountry",cleanCountry)



//  spark.udf.register("speciesUDF", species("Species"))



//    StructType([StructField("Species", StringType,
//  StructField("Category", StringType,
//  StructField("Period", StringType,
//  StructField("Annual percentage change", DoubleType
//  ])


}
