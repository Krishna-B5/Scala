package packJoin

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object Join {
  
 def main(args: Array[String]){
    
    println("===== Started =====\n")
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    println("====== table1 =======\n")
    val df1 = spark.read.format("csv")
                .option("header", "true")
                .load("file:///c:/data/join1.csv")
    df1.show()
    println("====== table2 =======\n")
    val df2 = spark.read.format("csv")
                .option("header", "true")
                .load("file:///c:/data/join2.csv")
    df2.show()
    
    println("====== INNER Join ======\n")
    val joindf = df1.join(df2, df1("txnno") === df2("tno"), "inner").show()
    
    println("====== Left Join =======\n")
    val leftdf = df1.join(df2, df1("txnno") === df2("tno"), "left").show()
    
    println("====== Left Semi Join ======\n")
    val leftsemidf = df1.join(df2, df1("txnno") === df2("tno"), "leftsemi").show()
    
    println("====== Left Anti Join ======\n")
    val leftantidf = df1.join(df2, df1("txnno") === df2("tno"), "leftanti").show()
    
    println("====== Right Join =======\n")
    val rightdf = df1.join(df2, df1("txnno") === df2("tno"), "right").show()
    
    println("====== Full Join =======\n")
    val fulldf = df1.join(df2, df1("txnno") === df2("tno"), "full")
                .withColumn("txnno", expr("case when txnno is null then tno else txnno end"))
                .drop(col("tno"))
                .orderBy("txnno")
                .show()
    println("====== Done =====\n")
 }
}