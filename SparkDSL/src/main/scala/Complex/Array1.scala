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
  
	val flatdf = jsondf.select(col("Address.*"),
//	                           col("address.Permanent address").as("Per"),
//	                           col("address.current address").as("Cur"),
	                           col("Mobile"),
	                           col("Name"),
	                           explode(col("Pets")).as("pet"),
	                           col("status")  
	                           )
	println("===== DataFrame ======")           
	flatdf.show()
	println("====== Schema ========")
	flatdf.printSchema()
  }
}