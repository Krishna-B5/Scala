package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object WithoutSQL {

  def main(args: Array[String]) {

    println("===== Started =====")
    println
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val usdf = spark.read.format("CSV")
      .option("header", "true")
      .load("file:///c:/data/usdata.csv")

    //usdf.show()
   val fdf = usdf.select("first_name", "last_name")
   fdf.show()
    
    println("===== Done =====")

  }
}