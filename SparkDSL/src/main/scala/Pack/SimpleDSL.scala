package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object SimpleDSL {

  def main(args: Array[String]) {

    println("===== Started =====")
    println

    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val dtdf = spark.read.format("CSV")
              .option("header", "true")
              .load("file:///c:/data/dt.txt")

    dtdf.show()
    
    println("====== Filtered data by AND ======\n")
    val fildf = dtdf.filter(col("category").contains("Gymnastics")
                            &&
                            col("spendby") === "cash")
    fildf.show()
    
    println("====== Filtered data by OR ======\n")
    val fildf1 = dtdf.filter(col("category") === "Gymnastics"
                            ||
                            col("spendby") === "cash")
    fildf1.show()
    
    println("===== Contains/ LIKE operator =====\n")
    val likedf = dtdf.filter(col("product") like ("%Gymnastics%"))
    likedf.show()
    
    println("====== multivalue/ ISIN operator ======\n")
    val isindf = dtdf.filter(col("category") isin ("Gymnastics","Exercise"))
    isindf.show()
    
    println("===== NULL Values ======\n")
    val nulldf = dtdf.filter(col("id").isNull)
    nulldf.show()
    
    println("===== Not Null Values ======\n")
    val notdf = dtdf.filter(col("id").isNotNull)
//    or
//    val notdf = dtdf.filter( ! (col("id").isNUll))
    notdf.show()
   
    println("===== Done =====")
  }
}