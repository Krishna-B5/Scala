package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TaskReadingfile {
  
  def main(args: Array[String]){
    
      val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			
			println("======== file data=====")
			
			println("======== file data=====")
			
			val data = sc.textFile("file:///C:/data/txns")
			data.take(5).foreach(println)
			
			println("====== Filtered Data =====")
			val filterdata = data.filter(x => x.contains("Gymnastics"))
			filterdata.take(5).foreach(println)		
	
  }
}