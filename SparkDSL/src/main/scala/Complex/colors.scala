package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf

object colors {
  
  println("======= Started ==========")
  def main(args: Array[String]): Unit={
    
  val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  
  val df = spark.read.format("json")
           .option("multiline", "true")
           .load("file:///c:/data/CmJSON/colors.json")
   
   println("======== Orginal Data======")
   df.show()
   println("======== Orignal Schema =======")
   df.printSchema()
   
//   val arraydf = df.withColumn("Color", explode(col("colors")))
//                   .withColumn("RGBA", explode(col("rgba")))
//   
//   arraydf.printSchema()
//   
//   val flatdf = arraydf.select(col("category"),
//                               col("code.hex")
//                               )
//   flatdf.printSchema()
   
   println("======== Done =========")
  }
}