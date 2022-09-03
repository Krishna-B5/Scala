package Pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object AWS_S3 {

  def main(args: Array[String]): Unit = {

    println("================Started============")
    println
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("fs.s3a.access.key", "AKIA5TH4P6EXOZK3Z6HD")
      .option("fs.s3a.secret.key", "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")
      .load("s3a://zeyodevbb/datatxns.txt")

    df.show()

  }

}