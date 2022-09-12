package packJoin

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object Uber {

  def main(args: Array[String]) {
    
    println("====== Started =======\n")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val uberdf = spark.read.format("csv")
                  .option("header", "true")
                  .load("file:///c:/data/uber.csv")
    uberdf.show()
    
   val date1 = uberdf.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))
   
   date1.show()
   val finaldf = date1.withColumn("day", date_format(col("date"), "EE"))
                      .groupBy("dispatching_base_number", "day")
                      .agg(sum("trips").as("total") )
                      .orderBy("dispatching_base_number").show()
    
    println("===== Done =====\n")

  }
}