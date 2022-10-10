package FirstProject

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object First {
  
  
  def main(args: Array[String]): Unit={
    
    println("===== Started =======")
    
    val conf = new SparkConf().setAppName("Master").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val avrodf = spark.read.format("avro")
                 .load("file:///c:/data/projectsample.avro")
    
    println("======= Avro data ========")
    avrodf.show(5)
    
    println("======= URL data =========")
    val urldata = Source.fromURL("https://randomuser.me/api/0.8/?results=10")
    val urlstring = urldata.mkString
//    println(urlstring) 

//  converting string to rdd
    val rdd = sc.parallelize(List(urlstring))
//    rdd.foreach(println)
   
//  converting rdd to dataframe  
    val urldf = spark.read.json(rdd)
//    urldf.show()
    urldf.printSchema()
    
    println("======= Flattening the data =======")
    println("========First level flattening =====")
    val flatdf = urldf.select(col("nationality"),
                              explode(col("results")).as("Result"),
                              col("seed"),
                              col("version")
                              )
    flatdf.printSchema()
    
    println("========Final level flattening =====\n")
    val finaldf = flatdf.select(col("nationality"),
                                col("Result.user.cell"),
                                col("Result.user.dob"),
                                col("Result.user.email"),
                                col("Result.user.gender"),
                                col("Result.user.location.*"),
                                col("Result.user.md5"),
                                col("Result.user.name.*"),
                                col("Result.user.password"),
                                col("Result.user.phone"),
                                col("Result.user.picture.*"),
                                col("Result.user.registered"),
                                col("Result.user.salt"),
                                col("Result.user.salt"),
                                col("Result.user.sha256"),
                                col("Result.user.username"),
                                col("seed"),
                                col("version")
                                )
//    finaldf.show(false)
    finaldf.printSchema()
    
// removing the numeric  from Username column
    println("========= Removing the numeric  from Username colum =========")
    val rmdf = finaldf
    .withColumn("UserName", regexp_replace(col("username"), "([0-9])", ""))
    rmdf.show()
  
//    Broadcast Join
    println("============ Broadcast Join ==============")
    val joindf = avrodf.join(broadcast(rmdf),Seq("UserName"),"left")
    joindf.show()
    
// Nationality null
    println("======= Nationality null =============")
    val df1 = joindf.filter(col("nationality").isNull)
    df1.show()
   
// Nationality not null
    println("======= Nationality not null =========")
    val df2 = joindf.filter(col("nationality").isNotNull)
    df2.show()
    
// df1 Dataframe replace all the column nulls to "NOT AVAILABLE"
    println("========= Null to Not available ===========\n") 
    val notdf = df1.na.fill("Not Available").na.fill(0)
    notdf.show()
    
    println("============ Adding current date at end =========\n")
    val current_notdf = notdf.withColumn("Curent_Date", current_date)
    current_notdf.show()
    
    println
    val current_df = df2.withColumn("Curent_Date", current_date)
    current_df.show()
    
    println("====== Done ======")
  }
}