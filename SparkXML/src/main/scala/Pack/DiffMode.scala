package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DiffMode {
  
  def main(args: Array[String]){
    
    println("===== Started =====")
    println
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val df = spark.read.format("CSV")
                 .load("file:///c:/data/datatxns.txt")
    
    df.show()
    df.printSchema()
    
//    df.write.format("csv").save("file:///C:/data/mdata") 
//    df.write.format("csv").mode("append").save("file:///C:/data/mdata") 
//    df.write.format("csv").mode("overwrite").save("file:///C:/data/mdata")  
    df.write.format("csv").mode("ignore").save("file:///C:/data/mdata") 
    
     println("===== Done =====")
  
 }
}