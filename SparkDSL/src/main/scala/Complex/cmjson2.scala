package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window

object cmjson2 {
  
  def main(args: Array[String]): Unit={
    
    println("===== Started =======")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val jsondf = spark.read.format("json")
                 .option("multiline", "true")
                 .load("file:///c:/data/CmJSON/picturem.json")
    println("===== DataFrame ======")           
    jsondf.show()
    println("====== Schema ========")
    jsondf.printSchema()
    
    println("===== Flattening the data ========\n")
//    println("===== But the column name will be same height,url,width ======")
//    val flatdf = jsondf.select(
//                              col("id"),
//                              col("image.*"),
//                              col("name"),
//                              col("thumbnail.*"),
//                              col("type")
//                              )
    
    println("==== To differenitate the column name we have to give .as(alias)")
    val flatdf = jsondf.select(
                                col("id"),
                                col("image.height").as("image_height"),
                                col("image.url").as("image_url"),
                                col("image.width").as("image_width"),
                                col("name"),
                                col("thumbnail.height").as("thumbnail_height"),
                                col("thumbnail.url").as("thumbnail_url"),
                                col("thumbnail.width").as("thumbnail_width"),
                                col("type")
                                )
    println("===== DataFrame ======")           
    flatdf.show()
    println("====== Schema ========")
    flatdf.printSchema()
    
////   withColumn drawback is we will have original column too we should drop it
//     advantage is not need add all the columns 
//    println("======= withColumn ============\n")
//    val withdf = jsondf
//                 .withColumn("image_height", col("image.height"))
//                 .withColumn("image_url", expr("image.url"))
//                 .withColumn("image_width", expr("image.width"))
//                 .withColumn("thumbnail_height", expr("thumbnail.height"))
//                 .withColumn("thumbnail_url", expr("thumbnail.url"))
//                 .withColumn("thumbnail_width", expr("thumbnail.width"))
//                 .drop("image")
//                 .drop("thumbnail")
//    
//    
//    println("===== DataFrame ======")           
//    withdf.show()
//    println("====== Schema ========")
//    withdf.printSchema()
//    
    
    println("===== Done ========")

  }
}