package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object PartitionBy {
  
  def main(args: Array[String]){
    println("===== Started =====")
    println
    
    val conf = new SparkConf().setAppName("master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val usdf = spark.read.format("CSV").option("header", "true").
               load("file:///c:/data/usdata.csv")
               
    usdf.show()
    
    val filterdf = usdf.filter("age > 10")
    
    filterdf.write.format("CSV").mode("overwrite").partitionBy("state","county")
    .save("file:///c:/data/uspartition")
    
  }
}