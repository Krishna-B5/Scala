package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

case class schema(id:String,tdate:String,category:String,product:String)


object Gym {
   def main(args: Array[String]){
    
      val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			
			val spark = SparkSession.builder().getOrCreate() 
			import spark.implicits._
			
			println("======== file data=====")
			
			println("======== file data=====")
			
			val data = sc.textFile("file:///C:/data/datatxns.txt")
			data.foreach(println)
			
			println("====== Filtered Data =====")
			val filterdata = data.filter(x => x.contains("Gymnastics"))
			filterdata.foreach(println)		
			
			println
		  println("========Column 4th Gymnastics=======")
		  println
		  
		  val mapsplit = filterdata.map(x => x.split(",")) 
// we have not used flatMap we are not doing flattening here just we are splitting the data
		  
		  val schemadd = mapsplit.map(x => schema(x(0),x(1),x(2),x(3)))
		  
		  val filterdd = schemadd.filter(x => x.product.contains("Gymnastics") )
		  
		  val df = filterdd.toDF()
		  
      df.show()
      println("sql filter data")
      df.createOrReplaceTempView("customer")
      val dfdd = spark.sql("select * from customer where tdate ='06-26-2011'")
      dfdd.show()
		  //filterdd.foreach(println)
  }
}  