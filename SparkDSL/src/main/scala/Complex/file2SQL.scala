package Complex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.regex.Pattern
import org.apache.calcite.sql.JoinType
import org.apache.spark.sql.Column

object file2SQL {
  
  def main(args: Array[String]): Unit={
    
    println("========== Started ==========")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    println("===== File1 =======\n")
    val df1 = spark.read.format("csv")
              .option("header", "true")
              .option("delimiter", "~")
              .load("file:///c:/data/file1.csv")
    
    df1.show()
    
    println("===== File2 =======\n")
    val df2 = spark.read.format("csv")
              .option("header", "true")
              .option("delimiter", "~")
              .load("file:///c:/data/file2.csv")
    
    df2.show()
    
    val df11 = df1.withColumn("empnam", explode(split(col("empman"), pattern = "\\,")))
    
    df11.show()
    
    println("====== flatten data =========\n")
    
    val flatdf = df11
                 .join(df2, df11("empnam") === df2("mno"), joinType = "inner" )
                 .drop("empnam")
                 .drop("mno")
                 .groupBy("empno", "empman")
                 .agg(collect_list("mname"))
    
    flatdf.show()
    
    
    println("====== Done ========")
  }
}