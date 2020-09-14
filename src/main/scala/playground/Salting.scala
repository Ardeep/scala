package playground

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Salting extends App{

  val spark=SparkSession.builder()
    .appName("test1")
    .config("spark.master","local")
    .getOrCreate()

  val employeesDF = joinFun("employees")
  val salariesDF = joinFun("salaries")
  val managersDF = joinFun("dept_manager")

//  employeesDF.show(4)


  val salariesDFSalted= salariesDF
    .withColumn("dummy",monotonically_increasing_id %1000)
    .withColumn("emp_no_suffix",
      concat(col("emp_no"),lit("-"),col("dummy")))

  salariesDFSalted.show(10)

  import spark.implicits._

  val r = scala.util.Random

  //  Create a "population" dataset with the numbers between 0 and 1000

  val population:List[List[Int]] = for {
    i <- (0 to 1000).toList
    d <- 1 to 1
  } yield List(d,i)
  val df = population.map(x =>(x.head, x(1))).toDF(Seq("dummy_key","suffix"):_*)

  df.show(10)



  val employeesDFSalted = employeesDF.crossJoin(df).
    withColumn("emp_no_suffix", concat(col("emp_no"),lit("-"),col("suffix")))

  println("employeesDF")
  employeesDF.show(15)
  println("employeesDFSalter")
  employeesDFSalted.show(15)

  // Create the Execution Plan
  val joinDF = salariesDFSalted.join(employeesDFSalted,
    salariesDFSalted.col("emp_no_suffix") === employeesDFSalted.col("emp_no_suffix"))

    joinDF.show(15)


  def joinFun(table:String): sql.DataFrame ={
    var df = spark.read.format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", s"public.$table")
      .load()
    df
  }

}
