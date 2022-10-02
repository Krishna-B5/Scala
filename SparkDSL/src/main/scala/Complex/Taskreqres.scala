package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Taskreqres {
  
  def main(args: Array[String]): Unit={
    
    println("=========== Started =============")
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("json")
             .option("multiline", "true")
             .load("file:///c:/data/CmJSON/reqres.json")
    
    println("===== Original data =======")
    df.show()
    println("======== Original Schema ")
    df.printSchema()
    
    val arraydf = df.withColumn("data", explode(col("data")))
    
    println("===== Array data =======")
    arraydf.show()
    println("======== Array Schema ")
    arraydf.printSchema()
    
    val flatdf = arraydf.select(
                                "data.*",
                                "page",
                                "per_page",
                                "support.*",
                                "total",
                                "total_pages"
                                )
    println("===== Flatten data =======")
    flatdf.show()
    println("======== Flatten Schema ")
    flatdf.printSchema()
    
//    val finaldf = flatdf.select(col("page"),
//                                col("per_page"),
//                                struct(
//                                    col("text"),
//                                    col("url")
//                                    ).as("support"),
//                                col("total"),
//                                col("total_pages")
//                                )
    
    val finaldf = flatdf.withColumn("support", 
                                     struct(
                                         col("text"),
                                         col("url")
                                         ))
                        .drop("text")
                        .drop("url")
                        .groupBy("page", "per_page","support","total","total_pages")
                        .agg(collect_list(
                         struct("avatar","email","first_name","id","last_name")).as("data"))
    
    println("===== complex data =======")
    finaldf.show()
    println("======== complex Schema ")
    finaldf.printSchema()
   
    println("====== Done ========")
  }
}