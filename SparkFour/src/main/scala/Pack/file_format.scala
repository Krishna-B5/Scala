package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object file_format {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    println("====== Started =======")
    
    val avrod = spark.read.format("csv")
                .option("header", true)
                .load("file:///c:/data/Practise/dtdata.csv")
    avrod.show(5)
    
    avrod.createOrReplaceTempView("dept")
    
    val qdata = spark.sql("select * from dept where salary = 700")
    qdata.show()
  }
}