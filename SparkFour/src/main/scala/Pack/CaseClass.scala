package Pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class schema(id:String,date:String,product:String,Category:String)

object CaseClass {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val data = sc.textFile("c:/data/datatxns.txt")
    
    data.foreach(println)
    
    val mapsplit = data.map(x => x.split(","))
    
    val df = mapsplit.map(x => schema(x(0),x(1),x(2),x(3)))
    
    mapsplit.foreach(println)
    
    val df1 = df.toDF()
    
    df1.show()
    
    df1.createOrReplaceTempView("table1")
    
    val dfsql = spark.sql(" select * from table1 where product = 'Exercise' and date = '06-26-2011'")
    
    dfsql.show()

  }
}