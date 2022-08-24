package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object RowSchema {
  
  def main(args: Array[String]){
    
    println("===== started =====")
    println
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    println("===== Read file =====")
    val data = sc.textFile("file:///c:/data/datatxns.txt",1)
    data.foreach(println)
    
    println
    println("===== Filter data =====")
    val filterdata = data.filter(x => x.contains("Gymnastics"))
    filterdata.foreach(println)
    
    println
    println("===== Split data 4th column=====")
    val spdata = filterdata.map(x => x.split(","))
    val rowdd = spdata.map(x => Row( x(0),x(1),x(2),x(3)))
    // rowdd.foreach(println)
    val ftdata = rowdd.filter(x => x(3).toString().contains("Gymnastics"))
    ftdata.foreach(println)
    
    println
    println("===== Data frame =====")
    val structSchema = StructType(Array(
					StructField("id", StringType, true),
					StructField("tdate", StringType, true),
					StructField("category", StringType, true),
					StructField("product", StringType, true)));
    
    val df = spark.createDataFrame(ftdata, structSchema)
    
    df.show()
    
    df.createOrReplaceTempView("Item")
    
    val finaldate = spark.sql("select * from Item where tdate='06-26-2011'")
    
    finaldate.show()
        
  }
}