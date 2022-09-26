package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object flattoJson {
  
  def main(args: Array[String]): Unit={
    
    println("===== Started =======")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val jsondf = spark.read.format("json")
                 .option("multiline", "true")
                 .load("file:///c:/data/CmJSON/donut.json")
    println("===== DataFrame ======")           
    jsondf.show()
    println("====== Schema ========")
    jsondf.printSchema()
    
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
    
    val cmjsondf = flatdf.select(
                               col("id"),
                               struct(
                               col("image_height").as("height"),
                               col("image_url").as("url"),
                               col("image_width").as("width")
                               ).as("image"),   
                               col("name"),
                               struct(
                               col("thumbnail_height").as("height"),
                               col("thumbnail_url").as("url"),
                               col("thumbnail_width").as("width")
                               ).as("thumbnail"),
                               col("type")
                               )
    println("===== DataFrame ======")           
    cmjsondf.show()
    println("====== Schema ========")
    cmjsondf.printSchema()
  }
}