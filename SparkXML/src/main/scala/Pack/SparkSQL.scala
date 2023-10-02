package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkSQL {
  
  def main(args: Array[String]){
    
    println("===== Started =====")
    println
    
    val conf = new SparkConf().setAppName("master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.time(spark.read.format("jdbc")
             .option("url", "jdbc:mysql://zeyodb.czvjr3tbbrsb.ap-south-1.rds.amazonaws.com/zeyodb")
             .option("user", "root")
             .option("password", "Aditya908")
             .option("dbtable", "kgf")
             .option("driver", "com.mysql.cj.jdbc.Driver")
             .load()
    )
    
    df.show()
    df.printSchema()
    print("====== Done ======")
  }
}  