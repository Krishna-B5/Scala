package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SelectExprUSdata {
  
  def main(args: Array[String]){
    
    println("====== Started =======")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val dtdf = spark.read.format("csv")
              .option("header", "true")
              .load("file:///c:/data/usdata.csv")
    println("====== Printing the usdata ======\n")
    dtdf.show()
    
    val fildf = dtdf.filter(col("state") === "LA"
                            &&
                            col("age") > 10)
    println("===== Printing the data only state = LA and age > 10")
    fildf.show()
   println("===== Done ======") 
  }
}