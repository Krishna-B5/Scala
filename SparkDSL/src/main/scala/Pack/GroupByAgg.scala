package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object GroupByAgg {
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
    
    val finaldf = dtdf.groupBy(col("category"))
                      .agg(sum(col("amount")).as("total"))
                      .orderBy(col("category").desc)
                   
   finaldf.show()
   
   val finaldf1 = dtdf.groupBy(col("category"))
                       .agg(count(col("category")).as("count"))
                       .orderBy(col("count"))
   finaldf1.show()
  }
}