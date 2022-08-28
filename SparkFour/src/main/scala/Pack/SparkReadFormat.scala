package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkReadFormat {
  
  def main(args: Array[String]){
    
    println("===== started =====")
    println
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
     val csvdf = spark.read.format("csv").load("file:///C:/data/datatxns.txt")
     csvdf.show()
     
     val jsondf = spark.read.format("json").load("file:///C:/data/devices.csv")
     jsondf.show()
     
     val orcdf = spark.read.format("orc").load("file:///C:/data/data.orc")
     orcdf.show()	

     val pardf = spark.read.format("parquet").load("file:///C:/data/data.parquet")
     pardf.show()
  }
}