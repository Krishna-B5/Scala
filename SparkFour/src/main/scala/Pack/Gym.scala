package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

//case class schema(id:String,tdate:String,cat:String,product:String)
case class tschema(tdate:String,product:String)


object Gym {
   def main(args: Array[String]){
    
      val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			
			val spark = SparkSession.builder().getOrCreate() 
			import spark.implicits._
			
			println("======== file data=====")
			
			val data = sc.textFile("file:///C:/data/datatxns.txt")
			data.foreach(println)
			
	    println("======== without converting to DF using RDD filter=====")		
			println("====== Filtered Data =====")
			val filterdata = data.filter(x => x.contains("Gymnastics"))
			filterdata.foreach(println)		
			
			println("+++++ If it's column based then we have to split and impose schema ++++")
		  println("========Column 4th Gymnastics=======")
		  println
		  
		  val mapsplit = filterdata.map(x => x.split(","))  
// we have not used flatMap we are not doing flattening here just we are splitting the data
		  
//		  val schemadd = mapsplit.map(x => schema(x(0),x(1),x(2),x(3)))
		  val schemadd = mapsplit.map(x => tschema(x(1),x(3))) 
		  
//		  val filterdd = schemadd.filter(x => x.product.contains("Gymnastics") )
//		  filterdd.foreach(println)
		  
		  val df = schemadd.toDF()
		  
      df.show()
      println("sql filter data")
      df.createOrReplaceTempView("customer")
      val dfdd = spark.sql("select * from customer where tdate ='06-26-2011'")
      dfdd.show()
		  //filterdd.foreach(println)
  }
}  