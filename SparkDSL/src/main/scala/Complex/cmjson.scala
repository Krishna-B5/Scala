package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object cmjson {
  
  def main(args: Array[String]): Unit={
    println("===== Started ======")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    
    val jsondf = spark.read.format("json")
                 .option("multiline", "true")
                 .load("file:///c:/data/CmJSON/cm.json")
    jsondf.show()
    println("====== Schema ========")
    jsondf.printSchema()
    
    println("===== Flatting the data ======")
    val flatdf = jsondf.select("Technology",
    		                       "TrainerName",
    		                       "address.*",
//    		                       "address.temporary",
    		                        "id")
    flatdf.show()
    println("====== Schema ========")  
    flatdf.printSchema()
  }
}