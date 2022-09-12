package Pack

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row._

object WindowRanking {
  
  def main(args: Array[String]){
    
    println("====== Started ========")
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    println("====== Reading the data from file ======\n")
    val dtdf = spark.read.format("csv")
                .option("header", "true")
                .load("file:///c:/data/Psample.txt")
    dtdf.show()
   
    val winspec = Window
                  .partitionBy(col("department"))
                  .orderBy(col("salary").asc) 
    println("======= Printing the row number =========")
    dtdf.withColumn("Row_Num", row_number.over(winspec)).show()
    
    println("====== Printing rank wise =======\n")
    dtdf.withColumn("rank", rank.over(winspec)).show()
    
    println("====== Printing dense_rank wise =======\n")
    dtdf.withColumn("dense_rank", dense_rank.over(winspec))
        .filter("dense_rank=2").drop("dense_rank").show()
    
    println("====== Printing Percentage wise ======\n")
    dtdf.withColumn("Percent", percent_rank().over(winspec)).show()
    
    println("====== Printing ntile(b/w 2 values) wise ======")
    dtdf.withColumn("ntile", ntile(3).over(winspec)).show()
    println("====== Done ======")

  }
}