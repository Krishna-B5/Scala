package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

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
    
    val flatdf = jsondf.select(
                               col("Technology"),
                               col("TrainerName"),
                               col("address.permanent"),
                               col("address.temporary"),
                               col("id")
                              )
                              
//    val flatdf = jsondf.withColumn("permanent", col("address.permanent"))
////                     .withColumn("temporary", col("address.temporary"))
//                      .drop("address")

    
    flatdf.show()
    println("====== Schema ========")  
    flatdf.printSchema()
    
    println("===== We can reverse it back to complex data =====")
    
    val complexdata = flatdf.select(
                                    col("Technology"),
                                    col("TrainerName"),
                                    struct(
                                          col("permanent"),
                                          col("temporary")
                                          ).as("address"),
                                    col("id")
                                    )
   complexdata.show()
   complexdata.printSchema() 
                                    
                                    
    println("====== Done ========")
  }
}