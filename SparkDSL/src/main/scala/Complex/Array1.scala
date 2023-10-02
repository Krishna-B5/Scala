package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Array1 {
  
  def main(args: Array[String]): Unit={
    
	println("===== Started =======")
	val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
	val sc = new SparkContext(conf)
	sc.setLogLevel("Error")

	val spark = SparkSession.builder().getOrCreate()
	import spark.implicits._

	val jsondf = spark.read.format("json")
	             .option("multiline", "true")
	             .load("file:///c:/data/CmArray/pets.json")
	             
	println("===== DataFrame ======")           
	jsondf.show()
	println("====== Schema ========")
	jsondf.printSchema()
  
  
//	val flatdf = jsondf.select(
//	                           col("Address.Permanent address").as("Permament"),
//	                           col("Address.current Address").as("Current"),
//	                           col("Mobile"),
//	                           col("Name"),
//	                           explode(col("Pets").as("Pet")),
//	                           col("status")                           
//	                          )
	
	val flatdf = jsondf.withColumn("Per", col("Address.Permanent address"))
	                   .withColumn("Cur", col("Address.current Address"))
	                   .drop("Address")
	                   .withColumn("Pet", explode(col("Pets")))
	                   .drop("Pets")
	                       
	                   
	println("===== DataFrame ======")           
	flatdf.show()
	println("====== Schema ========")
	flatdf.printSchema()
	
	println("===== Reverse the flattend Data to complex data ======")
	
	val complexdata = flatdf.select(
	                                struct(
	                                col("Per").as("Permanent address"),
	                                col("Cur").as("current Address")
	                                ).as("Address"),
	                                col("Mobile"),
	                                col("Name"),
	                                col("Pet"),
	                                col("status")
	                                )
	                         .groupBy("Address","Mobile","Name","status")
                           .agg(collect_list("Pet").as("Pets"))
 complexdata.show()
 complexdata.printSchema()
 
// val complexdata1 = complexdata.groupBy("Address","Mobile","Name","status")
//                               .agg(collect_list("Pet").as("Pets"))
//                               
// complexdata1.show()
// complexdata1.printSchema()
 
  }
}