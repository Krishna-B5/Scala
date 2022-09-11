package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object SecondHighest {
  
  def main(args: Array[String]){
    
    println("===== Started =====\n")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val dtdf = spark.read.format("csv")
                .option("header", "true")
                .load("file:///c:/data/dtdata.csv")
               
    dtdf.show()
    
    println("===== Displaying the second highest salary =====\n")
    
    val secdf = Window
                .partitionBy(col("dept"))
                .orderBy(col("salary").desc)
    
    val finaldf = dtdf.withColumn("SHSalary", dense_rank() over secdf)
        //              .drop("SHSalary")
                      .orderBy("dept")
   
    finaldf.show()
    println("\n ===== Done =====")
    
  }
}