package pack

object usecase1 {
  
  def main(args:Array[String]){
    
    val liststr = List("Amazon-Jeff-America","Microsoft-Billgates-America","TCS-TATA-india","Reliance-Ambani-India")
    println("====== Oroginal List =====")
    liststr.foreach(println)
    println
    // converting to lowercase and checking for specific value
    
    println("===== Filtering the india records =====")
    val filterdata = liststr.filter( x => x.toLowerCase().contains("india"))
    filterdata.foreach(println)
    println
    println("===== Flattening the record with hypen =====")
    val flatdata = filterdata.flatMap( x => x.split("-") )
    flatdata.foreach(println)
    println
    println("===== Replace the india to local =====")
    val rpdata = flatdata.map( x => x.replace("india", "local") )
    rpdata.foreach(println)
    println
    println("===== Concat , Done to the string")
    val concatdata = rpdata.map( x => x.concat(", Done"))
    concatdata.foreach(println)
  }
}