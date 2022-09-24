package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

object revision {
  
  case class schema(
      txnno:    Int,
      txndate:  String,
      custno:   String,
      amount:   String,
      category: String,
      product:  String,
      city:     String,
      state:    String,
      spendby:  String)

  def main(args: Array[String]): Unit = {
    println("===== Started ========")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("=====file 1=====\n")
    
    val collist = List( 
		                  "txnno" ,
		                  "txndate" ,
		                  "custno" ,
		                  "amount" ,
		                  "category" ,
		                  "product" ,
		                  "city" ,
		                  "state" ,
		                  "spendby"
		                  )
		                  

    val file1 = sc.textFile("file:///c:/data/revdata/file1.txt")

    file1.take(5).foreach(println)

    val gymdata = file1.filter(x => x.contains("Gymnastics"))

    val mapsplit = gymdata.map(x => x.split(","))

    val schemardd = mapsplit.map(x => schema(
      x(0).toInt,
      x(1),
      x(2),
      x(3),
      x(4),
      x(5),
      x(6),
      x(7),
      x(8)))
      
    val progym = schemardd.filter(x => x.product.contains("Gymnastics"))

    val schemadf = progym.toDF().select(collist.map(col): _*)

    schemadf.show(5)
    
    val file2 = sc.textFile("file:///C:/data/revdata/file2.txt")

		val rowmapsplit = file2.map( x => x.split(","))

		val rowrdd = rowmapsplit.map( x => Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

		val schema1 = StructType(Array(
				StructField("txnno",IntegerType,true),
				StructField("txndate",StringType,true),
				StructField("custno",StringType,true),
				StructField("amount", StringType, true),
				StructField("category", StringType, true),
				StructField("product", StringType, true),
				StructField("city", StringType, true),
				StructField("state", StringType, true),
				StructField("spendby", StringType, true)
				))


		val rowdf = spark.createDataFrame(rowrdd, schema1).select(collist.map(col): _*)

		rowdf.show(5)

    println("====== Done =======")
  }
}