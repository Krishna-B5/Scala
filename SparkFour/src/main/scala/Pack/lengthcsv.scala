package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object lengthcsv {
  
  def main(args: Array[String]): Unit={
    
      val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			
			println("======== file data=====")
			
			println("======== file data=====")
			
			val data = sc.textFile("file:///C:/data/usdata.csv")
			data.take(5).foreach(println)
			
			println
			println("=====length data=====")
			println
			
			val fildata = data.filter( x => x.length > 200)
			fildata.foreach(println)
			
			println
			println("=====flatten data=====")
			println
			
			val flatdata = fildata.flatMap(x => x.split(","))
			flatdata.foreach(println)
			
			println
			println("=====replace data=====")
			println
			
			val replacedata = flatdata.map( x => x.replace("-", ""))
			replacedata.foreach(println)
			
			println
			println("=====concat data=====")
			println
			
			val concatdata = replacedata.map( x => x.concat(",zeyo"))
			concatdata.foreach(println)

			println("====Data Write=====")
			concatdata.coalesce(1).saveAsTextFile("file:///C:/data/33dir")
    
  }
}