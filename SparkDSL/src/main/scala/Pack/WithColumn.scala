package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WithColumn {
  
  def main(args: Array[String]) {
    
    println("====== Started ======\n")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // reading the file
    val dtdf = spark.read.format("CSV")
      .option("header", "true")
      .load("file:///c:/data/dt.txt")

    println("====== Data Read ======\n")
    dtdf.show()

   val finaldf = dtdf
      .withColumn("tdate", expr("split(tdate,'-')[2]"))
      .withColumnRenamed("tdate", "year")
      .withColumn("status", expr("case when spendby='cash' then 0 else 1 end")) 
    
   /*  val finaldf = dtdf
			.withColumn("wdate",expr("split(tdate,'-')[2]"))
			.withColumn("status",expr("case when spendby='cash' then 0 else 1 end")) */

    finaldf.show()
  }
}