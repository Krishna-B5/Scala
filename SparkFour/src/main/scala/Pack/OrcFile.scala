package Pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object OrcFile {

	def main(args:Array[String]):Unit={

			println("================Started============")
			println
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._

// Reading the JSON file
			val jsondf = spark.read.format("json")
			.load("file:///C:/data/devices.csv")  // your path 
      jsondf.show()
			
			jsondf.createOrReplaceTempView("jdf") // creating a tablename
			
			val finaldf = spark.sql("select * from jdf where lat>40")
			
			finaldf.show()
// Writing ORC file			
			finaldf.write.format("orc").mode("overwrite")
			.save("file:///C:/data/orcw")    // your path
			
			println("=====Done===")	

	}
}