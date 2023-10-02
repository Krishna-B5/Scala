package pack

object task1 {
  
  def main(args: Array[String]):Unit={
    
    println("===== List Operations =====")
    
    val liststr = List("Bigdata","spark","hive")
    liststr.foreach(println)
    val mapstr = liststr.map( x => x + "~Zeyo")  //concatenate the string
    mapstr.foreach(println)
    val splitmap = mapstr.flatMap( x => x.split("~"))
    splitmap.foreach(println)
    
  }
}