package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Array_withcol {
  
  def main(args: Array[String]): Unit={
    
    println("===== Started =======\n")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val readdf = spark.read.format("json")
                 .option("multiline", "true")
                 .load("file:///c:/data/CmArray/pets.json")
                
    println("====== normal data =====")
    readdf.show()
    println("====== normal schema =====")
    readdf.printSchema()
    
    println("====== withcolumn =====\n")
    
    val flatdf = readdf
                 .withColumn("Permanent address", expr("Address.`Permanent address`"))
                 .withColumn("Current address", expr("Address.`current address`"))
                 .withColumn("pets", explode(col("Pets")))
                 .drop("Address")
    
    println("====== withColumn data =====")
    flatdf.show()
    println("====== withColumn schema =====")
    flatdf.printSchema()

    
    println("====== Done ============")
  }
}