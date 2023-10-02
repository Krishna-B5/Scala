package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object XML1 {
  
  def main(args: Array[String]){
    
    println("===== Started =====")
    println
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val xmldata = spark.read.format("xml")
                  .option("rowTag", "book")
                  .load("file:///c:/data/book.xml")
    
    xmldata.show()
    xmldata.printSchema()
    
     println("===== Done =====")
  }
 
}