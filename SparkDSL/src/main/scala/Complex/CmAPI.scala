package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source


object CmAPI {
  
  def main(args: Array[String]): Unit={
    
    println("===== Started =======\n")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    println("========= URL API Data ========")
    val html = Source.fromURL("https://randomuser.me/api/0.8/?results=10")
    val urlstring = html.mkString
    println(urlstring)
    
    val rdd = sc.parallelize(List(urlstring))
    val df = spark.read.json(rdd)
    
    df.show()
    df.printSchema()
    
    println("===== First level of flattening =====")
    
    val flatdf = df.select(col("nationality"), 
                           explode(col("results")).as("Results"),
                           col("seed"),
                           col("version")
                           )
    flatdf.show()
    flatdf.printSchema()
    
    println("===== Second level of flattening =====\n")
    
    val finaldf = flatdf.select(col("nationality"),
                                col("results.user.cell"),
					                      col("results.user.dob"),
					                      col("results.user.email"),
					                      col("results.user.gender"),
                      					col("results.user.location.*"),
                      					col("results.user.md5"),
                      					col("results.user.name.*"),
                      					col("results.user.password"),
                      					col("results.user.phone"),
                      					col("results.user.picture.*"),
                      					col("results.user.registered"),
                      					col("results.user.salt"),
                      					col("results.user.sha1"),
                      					col("results.user.sha256"),
                      					col("results.user.username"),
                      					col("seed"),
                      					col("version")
					                      ).withColumn("today", current_date)
		
		finaldf.write.format("csv")
			     .partitionBy("today")
			     .mode("append")
			     .save("file:///C:/data/urldata")

                               
    println("======= Done===========")
  }
  
}