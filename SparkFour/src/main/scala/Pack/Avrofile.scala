package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Avrofile {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    println("====== Started ======")
    println
 // Reading Avro file   
    val avrodata = spark.read.format("avro").load("file:///c:/data/data.avro")
    avrodata.show()
    
    avrodata.createOrReplaceTempView("emp") // creating a table
    
    val filterdata = spark.sql("select * from emp where age = 9")
    
    filterdata.show()
    
    filterdata.write.format("csv").mode("overwrite").save("file:///c:/data/dataavro")
    
    println("===== Done =====")
    
  }
}