package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object test {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val data = sc.textFile("file:///c:/data/csv/*")
    
//    val data = sc.wholeTextFiles("file:///c:/data/csv/*")
    
    data.foreach(f => {
                        println(f)})
    }
    
  }