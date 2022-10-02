package revision

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Type2Struct {
  
  def main(args: Array[String]): Unit={
    
    println("====== Started =======")
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("json")
             .option("multiline", "true")
             .load("file:///c:/data/CmJSON/cm.json")
    
    println("====== Data frame ======")
    df.show()
    println("========= original schema =========")
    df.printSchema()
    
    val flatdf = df.select(col("Technology"),
                           col("TrainerName"),
//                           col("address.*"),
                           col("address.permanent").as("P"),
                           col("address.temporary").as("T"),
                           col("id")
                           )
                           
    println("======= Flatten Data ========")
    flatdf.show()
    println("======= Flatten Schema ======")
    flatdf.printSchema()
    
    println("========== Done ==========")
  }
}