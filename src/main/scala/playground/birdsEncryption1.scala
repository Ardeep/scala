package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object birdsEncryption1 extends App {

  val spark = SparkSession.builder
    .appName("birdsEncryption1")
    .config("spark.master","local")
    .getOrCreate()

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

  val df = spark.read.format("csv").
    option("header","true").
    schema(input_schema).
    load("/Users/debasmitadas/Documents/spark-essentials-master/src/main/resources/data/Birds/birds.csv")


//  def sha256Hash(text: String) : String = String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").
//    digest(text.getBytes("UTF-8"))))

  // SHA256 is not an encryption function it's a Hash Function. It cannot be reversed

  def sha256Hash: String=>String  = (text: String) => {
    val str2 = String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").
    digest(text.getBytes("UTF-8"))))
    str2
  }

  val speciesUDF = spark.udf.register("speciesUDF",sha256Hash)


  val df2 = df.withColumn("EncryptedSpecies",speciesUDF(col("Species")))
      .withColumn("EncryptedSpecies1",speciesUDF(col("Species")))
  df2.show(10)
}
