package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import part2dataframes.ColumnsAndExpressions.carsDF

object ColumnsAndExpressions1 extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions1")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF= spark.read.format("json").option("inferSchema","true")
    .load("/Users/debasmitadas/Documents/spark-essentials-master/src/main/resources/data/cars.json")


  carsDF.printSchema()
  val newDF1=carsDF.select(col("Origin"),col("Acceleration"),
    expr("Cylinders").as("new field"),
    expr("Horsepower * 1000").as("NewHorsePower"))
  println(newDF1.show(5))

  newDF1.show(2)

  val newDF2 = carsDF.select(col("Year")).groupBy("Year")


//  val newDF4 = spark.sql("select * from carsDF1 Limit 10")
//  newDF4.show()

  val newDF5 = carsDF.withColumnRenamed("Acceleration","AccelerationRen")
  newDF5.show(5)

  val time1 = System.currentTimeMillis()

  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")

  println(s"Time taken without SQL: ${System.currentTimeMillis() - time1}")

  val time2 = System.currentTimeMillis()
  carsDF.createOrReplaceTempView("carsDF1")
  val europeanCarsDF2 = spark.sql("select * from carsDF1 where Origin <> 'USA' ")
  println(s"Time taken with SQL: ${System.currentTimeMillis() - time2}")


  println("European Cars:")
  europeanCarsDF.show(2)

  carsDF.filter((col("Origin")==="USA")).filter(col("Horsepower") > 150 ).show(2)
  carsDF.filter((col("Origin")==="USA").and(col("Horsepower") > 150 )).show(2)

  carsDF.select("Origin").distinct()

  val moviesDF= spark.read.format("json")
    .load("/Users/debasmitadas/Documents/spark-essentials-master/src/main/resources/data/movies.json")

  moviesDF.printSchema()

  moviesDF.select()
  moviesDF.withColumn("total gross", expr("Worldwide_Gross + US_Gross").as("Total Gross"))
    .show(4)

  moviesDF.withColumn("total gross",col("Worldwide_Gross") + col("US_Gross"))
      .show(4)


  moviesDF.filter(col("Major_Genre") === "COMEDY").filter(col("IMDB_Rating")>6)
    .show(2)


}
