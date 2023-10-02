package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SelectExprAndColumn {

  def main(args: Array[String]) {
    
    println("====== Started ======\n")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    // reading the file
    val dtdf = spark.read.format("CSV")
      .option("header", "true")
      .load("file:///c:/data/dt.txt")

    println("====== Data Read ======\n")
    dtdf.show()
    // Reading the data using SELECT
    val sedf = dtdf.select("id",
                            "tdate",
                            "amount",
                            "category",
                            "product",
                            "spendby")
    println("===== Data read using SELECT ===== \n")
    sedf.show()
    
    // Reading data using selectExpr year and adding status column at the end
    val exprdf = dtdf.selectExpr("id",
                                 "split(tdate,'-')[2] as year",
                                 "tdate",
                                 "amount",
                                 "category",
                                 "product",
                                 "spendby",
                                 "case when spendby = 'cash' then 0 else 1 end as status"
                                 )
    println("====== tdate to year and status column at the end")
    exprdf.show()
    println("===== Done =====")
  }
}